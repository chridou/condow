//! [BytesStream] for a single [PartRequest].

use std::{task::Poll, time::Instant};

use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use tracing::{debug_span, Span};

use crate::{
    condow_client::CondowClient,
    errors::CondowError,
    machinery::part_request::PartRequest,
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesStream, BytesStreamItem},
    InclusiveRange,
};

pin_project! {
    /// A stream which returns [ByteStreamItem]s for a [PartRequest].
    ///
    /// It does a request via the client and returns the [Bytes] of that request's
    /// [ByteStream]
    ///
    /// The supplied [Probe] is notified on part events and chunk events.
    /// Global download events are not published.
    pub struct PartBytesStream<P: Probe> {
        part_request: PartRequest,
        state: State,
        probe: P,
        started_at: Instant,
        part_span: Span,
    }
}

impl<P> PartBytesStream<P>
where
    P: Probe + Clone,
{
    #[allow(dead_code)]
    pub(crate) fn from_client<C: CondowClient>(
        client: &ClientRetryWrapper<C>,
        location: C::Location,
        part_request: PartRequest,
        probe: P,
        parent: &Span,
    ) -> Self {
        let get_part_stream = {
            let probe = probe.clone();
            move |range: InclusiveRange| {
                client
                    .download(location.clone(), range, probe.clone())
                    .boxed()
            }
        };

        Self::new(&get_part_stream, part_request, probe, parent)
    }

    pub fn new(
        get_part_stream: &dyn Fn(
            InclusiveRange,
        ) -> BoxFuture<'static, Result<BytesStream, CondowError>>,
        part_request: PartRequest,
        probe: P,
        parent: &Span,
    ) -> Self {
        let range = part_request.blob_range;
        probe.part_started(part_request.part_index, range);

        let part_span = debug_span!(parent: parent,
                "download_part", 
                part_index = %part_request.part_index,
                part_range = %part_request.blob_range,
                part_offset = %part_request.range_offset);

        PartBytesStream {
            part_request,
            state: State::GettingStream(get_part_stream(range)),
            probe,
            started_at: Instant::now(),
            part_span,
        }
    }
}

struct StreamingPart {
    bytes_stream: BytesStream,
    chunk_index: usize,
    blob_offset: u64,
    range_offset: u64,
    bytes_left: u64,
}

enum State {
    /// A future to yield a [BytesStream] was created. It needs to be polled
    /// until it retuns the stream.
    GettingStream(BoxFuture<'static, Result<BytesStream, CondowError>>),
    Streaming(StreamingPart),
    /// Nothing to do anymore. Always return `None`.
    Finished,
}

impl<P> Stream for PartBytesStream<P>
where
    P: Probe + Clone,
{
    type Item = BytesStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        let _span_gurad = this.part_span.enter();

        // We need to get ownership of the state. So we have to reassign it in each match
        // arm unless we want to be in "Finished" state.
        let state = std::mem::replace(this.state, State::Finished);

        match state {
            State::GettingStream(mut fut) => match fut.poll_unpin(cx) {
                Poll::Pending => {
                    *this.state = State::GettingStream(fut);
                    Poll::Pending
                } // Nothing there. Poll again later. Future will wake us up.
                Poll::Ready(Ok(bytes_stream)) => {
                    let part_state = StreamingPart {
                        bytes_stream,
                        chunk_index: 0,
                        blob_offset: this.part_request.blob_range.start(),
                        range_offset: this.part_request.range_offset,
                        bytes_left: this.part_request.blob_range.len(),
                    };

                    *this.state = State::Streaming(part_state);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Err(err)) => {
                    this.probe.part_failed(
                        &err,
                        this.part_request.part_index,
                        &this.part_request.blob_range,
                    );
                    *this.state = State::Finished;
                    Poll::Ready(Some(Err(err)))
                }
            },
            State::Streaming(mut streaming_state) => {
                match streaming_state.bytes_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        let bytes_len = bytes.len() as u64;

                        if bytes_len > streaming_state.bytes_left {
                            let err = CondowError::new_io("Too many bytes received");
                            this.probe.part_failed(
                                &err,
                                this.part_request.part_index,
                                &this.part_request.blob_range,
                            );
                            *this.state = State::Finished;
                            return Poll::Ready(Some(Err(err)));
                        }

                        streaming_state.bytes_left -= bytes_len;

                        this.probe.chunk_received(
                            this.part_request.part_index,
                            streaming_state.chunk_index,
                            bytes_len as usize,
                        );

                        streaming_state.chunk_index += 1;
                        streaming_state.blob_offset += bytes_len;
                        streaming_state.range_offset += bytes_len;

                        *this.state = State::Streaming(streaming_state);
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    Poll::Ready(Some(Err(err))) => {
                        this.probe.part_failed(
                            &err,
                            this.part_request.part_index,
                            &this.part_request.blob_range,
                        );

                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                    Poll::Ready(None) => {
                        if streaming_state.bytes_left == 0 {
                            this.probe.part_completed(
                                this.part_request.part_index,
                                streaming_state.chunk_index,
                                this.part_request.blob_range.len(),
                                this.started_at.elapsed(),
                            );

                            *this.state = State::Finished;

                            Poll::Ready(None)
                        } else {
                            let err = CondowError::new_io("unexpected end of part chunks");
                            this.probe.part_failed(
                                &err,
                                this.part_request.part_index,
                                &this.part_request.blob_range,
                            );

                            *this.state = State::Finished;

                            Poll::Ready(Some(Err(err)))
                        }
                    }
                    Poll::Pending => {
                        *this.state = State::Streaming(streaming_state);
                        Poll::Pending
                    }
                }
            }
            State::Finished => Poll::Ready(None),
        }
    }
}
