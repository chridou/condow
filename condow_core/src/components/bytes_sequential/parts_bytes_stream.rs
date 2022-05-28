//! [BytesStream] for multiple [PartRequest]s
//!
use std::{
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use tracing::Span;

use crate::{
    components::part_request::PartRequest,
    condow_client::CondowClient,
    config::LogDownloadMessagesAsDebug,
    errors::CondowError,
    machinery::DownloadSpanGuard,
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesStream, BytesStreamItem},
    InclusiveRange,
};

use super::PartBytesStream;

/// Internal state of the stream.
enum State<P: Probe> {
    /// We are streming the [Chunk]s of a part.
    Streaming(PartBytesStream<P>),
    /// Nothing more to do. Always return `None`
    Finished,
}

pin_project! {
    /// A stream which returns [ChunkStreamItem]s for all [PartRequest]s of a download.
    ///
    /// Parts are downloaded sequentially
    pub struct PartsBytesStream<P: Probe> {
        get_part_stream: Box<dyn Fn(InclusiveRange) -> BoxFuture<'static, Result<BytesStream, CondowError>> + Send + 'static>,
        part_requests: Box<dyn Iterator<Item=PartRequest> + Send + 'static>,
        state: State<P>,
        probe: P,
        download_started_at: Instant,
        log_dl_msg_dbg: LogDownloadMessagesAsDebug,
        parent_span: Arc<Span>,
    }
}

impl<P> PartsBytesStream<P>
where
    P: Probe + Clone,
{
    pub fn new<I, L, F>(
        get_part_stream: F,
        mut part_requests: I,
        probe: P,
        log_dl_msg_dbg: L,
        parent_span: Arc<Span>,
    ) -> Self
    where
        I: Iterator<Item = PartRequest> + Send + 'static,
        L: Into<LogDownloadMessagesAsDebug>,
        F: Fn(InclusiveRange) -> BoxFuture<'static, Result<BytesStream, CondowError>>
            + Send
            + 'static,
    {
        let log_dl_msg_dbg = log_dl_msg_dbg.into();

        if let Some(part_request) = part_requests.next() {
            let stream =
                PartBytesStream::new(&get_part_stream, part_request, probe.clone(), &parent_span);

            Self {
                get_part_stream: Box::new(get_part_stream),
                part_requests: Box::new(part_requests),
                state: State::Streaming(stream),
                probe,
                download_started_at: Instant::now(),
                log_dl_msg_dbg,
                parent_span,
            }
        } else {
            probe.download_completed(Duration::ZERO);

            log_dl_msg_dbg.log("download (empty) completed");

            Self {
                get_part_stream: Box::new(get_part_stream),
                part_requests: Box::new(part_requests),
                state: State::Finished,
                probe,
                download_started_at: Instant::now(),
                log_dl_msg_dbg,
                parent_span,
            }
        }
    }

    pub(crate) fn from_client<C, I, L>(
        client: ClientRetryWrapper<C>,
        location: C::Location,
        part_requests: I,
        probe: P,
        log_dl_msg_dbg: L,
        parent_span: Arc<Span>,
    ) -> Self
    where
        I: Iterator<Item = PartRequest> + Send + 'static,
        L: Into<LogDownloadMessagesAsDebug>,
        C: CondowClient,
    {
        let get_part_stream = {
            let probe = probe.clone();
            move |range: InclusiveRange| {
                client
                    .download(location.clone(), range.into(), probe.clone())
                    .boxed()
            }
        };

        Self::new(
            get_part_stream,
            part_requests,
            probe,
            log_dl_msg_dbg,
            parent_span,
        )
    }
}

impl<P> Stream for PartsBytesStream<P>
where
    P: Probe + Clone,
{
    type Item = BytesStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        // We need to get ownership of the state. So we have to reassign it in each match
        // arm unless we want to be in "Finished" state.
        let state = std::mem::replace(this.state, State::Finished);

        match state {
            State::Streaming(mut part_stream) => {
                match part_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        *this.state = State::Streaming(part_stream);
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    Poll::Ready(Some(Err(err))) => {
                        let err: CondowError = err.into();
                        this.probe
                            .download_failed(Some(this.download_started_at.elapsed()));
                        this.log_dl_msg_dbg.log(format!("download failed: {err}"));
                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                    Poll::Ready(None) => {
                        if let Some(part_request) = this.part_requests.next() {
                            let stream = PartBytesStream::new(
                                this.get_part_stream,
                                part_request,
                                this.probe.clone(),
                                &this.parent_span,
                            );
                            *this.state = State::Streaming(stream);
                            cx.waker().wake_by_ref(); // Bytes Stream returned "Ready" and will not wake us up!
                            Poll::Pending
                        } else {
                            this.probe
                                .download_completed(this.download_started_at.elapsed());
                            this.log_dl_msg_dbg.log("download completed");
                            *this.state = State::Finished;
                            Poll::Ready(None)
                        }
                    }
                    Poll::Pending => {
                        *this.state = State::Streaming(part_stream);
                        Poll::Pending
                    }
                }
            }
            State::Finished => Poll::Ready(None),
        }
    }
}
