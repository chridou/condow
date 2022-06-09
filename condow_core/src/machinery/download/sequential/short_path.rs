use std::{sync::Arc, task::Poll, time::Instant};

use futures::Stream;
use pin_project_lite::pin_project;
use tracing::Span;

use crate::{
    condow_client::ClientBytesStream,
    condow_client::CondowClient,
    config::LogDownloadMessagesAsDebug,
    machinery::{
        configure_download::DownloadConfiguration, download::active_pull,
        part_request::PartRequest, DownloadSpanGuard,
    },
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesHint, BytesStream, BytesStreamItem},
};

pub(crate) async fn short_path<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    mut configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> BytesStream {
    let download_started_at = Instant::now();

    let exact_bytes = configuration.exact_bytes();
    let log_dl_msg_dbg = configuration.config.log_download_messages_as_debug;
    let ensure_active_pull = configuration.config.ensure_active_pull;
    let location = configuration.location;
    let part_request = configuration.part_requests.next().unwrap();
    let stream = match client
        .download(location, part_request.blob_range, probe.clone())
        .await
    {
        Ok(stream) => stream,
        Err(err) => {
            probe.part_failed(&err, 0, &part_request.blob_range);
            probe.download_failed(Some(download_started_at.elapsed()));
            log_dl_msg_dbg.log(format!("download failed: {err}"));

            return BytesStream::once_err(err);
        }
    };

    probe.part_started(part_request.part_index, part_request.blob_range);

    let stream = ShortPathTerminator::new(
        stream,
        part_request,
        probe.clone(),
        download_started_at,
        log_dl_msg_dbg,
        download_span_guard.shared_span(),
    );

    if *ensure_active_pull {
        let active_stream = active_pull(stream, probe, log_dl_msg_dbg);
        BytesStream::new_active_stream(active_stream, BytesHint::new_exact(exact_bytes))
    } else {
        BytesStream::new_short_path(stream, BytesHint::new_exact(exact_bytes))
    }
}

pin_project! {
pub struct ShortPathTerminator {
    #[pin]
    stream: ClientBytesStream,
    probe: Box<dyn Probe>,
    download_started_at: Instant,
    log_dl_msg_dbg: LogDownloadMessagesAsDebug,
    parent_span: Arc<Span>,
    part_request: PartRequest,
    chunk_index: usize,
}
}

impl ShortPathTerminator {
    pub fn new<P: Probe, L: Into<LogDownloadMessagesAsDebug>>(
        stream: ClientBytesStream,
        part_request: PartRequest,
        probe: P,
        download_started_at: Instant,
        log_dl_msg_dbg: L,
        parent_span: Arc<Span>,
    ) -> Self {
        Self {
            stream,
            part_request,
            probe: Box::new(probe),
            download_started_at,
            log_dl_msg_dbg: log_dl_msg_dbg.into(),
            parent_span,
            chunk_index: 0,
        }
    }
}

impl Stream for ShortPathTerminator {
    type Item = BytesStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(err))) => {
                this.probe
                    .part_failed(&err, 0, &this.part_request.blob_range);
                this.probe
                    .download_failed(Some(this.download_started_at.elapsed()));
                this.log_dl_msg_dbg.log(format!("download failed: {err}"));
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                this.probe.part_completed(
                    this.part_request.part_index,
                    *this.chunk_index,
                    this.part_request.blob_range.len(),
                    this.download_started_at.elapsed(),
                );

                this.probe
                    .download_completed(this.download_started_at.elapsed());
                this.log_dl_msg_dbg.log("download completed");

                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
