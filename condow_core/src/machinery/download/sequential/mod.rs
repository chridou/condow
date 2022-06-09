//! Components for sequential downloads
use crate::{
    condow_client::CondowClient,
    machinery::{configure_download::DownloadConfiguration, DownloadSpanGuard},
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesHint, BytesStream, ChunkStream},
};

use super::active_pull;

pub(crate) use download_parts_seq::DownloadPartsSeq;
pub(crate) use parts_bytes_stream::PartsBytesStream;
pub(crate) use short_path::ShortPathTerminator;

pub mod part_bytes_stream;
pub mod parts_bytes_stream;
mod short_path;

/// Download the parts sequentially.
///
/// The download is driven by the returned stream.
pub(crate) fn download_chunks_sequentially<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> ChunkStream {
    let ensure_active_pull = configuration.config.ensure_active_pull;
    let log_dl_msg_dbg = configuration.config.log_download_messages_as_debug;

    let bytes_hint = BytesHint::new_exact(configuration.exact_bytes());
    let stream = download_parts_seq::DownloadPartsSeq::from_client(
        client,
        configuration.location,
        configuration.part_requests,
        probe.clone(),
        log_dl_msg_dbg,
        download_span_guard,
    );

    if *ensure_active_pull {
        let active_stream = active_pull(stream, probe, log_dl_msg_dbg);
        ChunkStream::from_active_stream(active_stream, bytes_hint)
    } else {
        ChunkStream::from_download_parts_seq(stream, bytes_hint)
    }
}

pub(crate) async fn download_bytes_sequentially<C: CondowClient, P: Probe + Clone>(
    client: ClientRetryWrapper<C>,
    configuration: DownloadConfiguration<C::Location>,
    probe: P,
    download_span_guard: DownloadSpanGuard,
) -> BytesStream {
    if configuration.part_requests.parts_hint() == 1 {
        return short_path::short_path(client, configuration, probe, download_span_guard).await;
    }

    let ensure_active_pull = configuration.config.ensure_active_pull;
    let log_dl_msg_dbg = configuration.config.log_download_messages_as_debug;

    let exact_bytes = configuration.exact_bytes();

    let stream = PartsBytesStream::from_client(
        client,
        configuration.location,
        configuration.part_requests,
        probe.clone(),
        log_dl_msg_dbg,
        download_span_guard.shared_span(),
    )
    .exact_bytes(exact_bytes);

    if *ensure_active_pull {
        let active_stream = active_pull(stream, probe, log_dl_msg_dbg);
        BytesStream::new_active_stream(active_stream, BytesHint::new_exact(exact_bytes))
    } else {
        BytesStream::new_parts_bytes_stream(stream)
    }
}
mod download_parts_seq {
    use std::{
        sync::Arc,
        task::Poll,
        time::{Duration, Instant},
    };

    use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
    use pin_project_lite::pin_project;

    use crate::{
        condow_client::{ClientBytesStream, CondowClient},
        config::LogDownloadMessagesAsDebug,
        errors::CondowError,
        machinery::{download::PartChunksStream, part_request::PartRequest, DownloadSpanGuard},
        probe::Probe,
        retry::ClientRetryWrapper,
        streams::ChunkStreamItem,
        InclusiveRange,
    };
    /// Internal state of the stream.
    enum State {
        /// We are streming the [Chunk]s of a part.
        Streaming(PartChunksStream),
        /// Nothing more to do. Always return `None`
        Finished,
    }

    pin_project! {
        /// A stream which returns [ChunkStreamItem]s for all [PartRequest]s of a download.
        ///
        /// Parts are downloaded sequentially
        pub (crate) struct DownloadPartsSeq {
            get_part_stream: Box<dyn Fn(InclusiveRange) -> BoxFuture<'static, Result<ClientBytesStream, CondowError>> + Send + 'static>,
            part_requests: Box<dyn Iterator<Item=PartRequest> + Send + 'static>,
            state: State,
            probe: Arc<dyn Probe>,
            download_started_at: Instant,
            log_dl_msg_dbg: LogDownloadMessagesAsDebug,
            download_span_guard: DownloadSpanGuard,
        }
    }

    impl DownloadPartsSeq {
        pub fn new<I, L, F>(
            get_part_stream: F,
            mut part_requests: I,
            probe: Arc<dyn Probe>,
            log_dl_msg_dbg: L,
            download_span_guard: DownloadSpanGuard,
        ) -> Self
        where
            I: Iterator<Item = PartRequest> + Send + 'static,
            L: Into<LogDownloadMessagesAsDebug>,
            F: Fn(InclusiveRange) -> BoxFuture<'static, Result<ClientBytesStream, CondowError>>
                + Send
                + 'static,
        {
            let log_dl_msg_dbg = log_dl_msg_dbg.into();

            if let Some(part_request) = part_requests.next() {
                let stream = PartChunksStream::new(
                    &get_part_stream,
                    part_request,
                    probe.clone(),
                    download_span_guard.span(),
                );

                Self {
                    get_part_stream: Box::new(get_part_stream),
                    part_requests: Box::new(part_requests),
                    state: State::Streaming(stream),
                    probe,
                    download_started_at: Instant::now(),
                    log_dl_msg_dbg,
                    download_span_guard,
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
                    download_span_guard,
                }
            }
        }

        pub fn from_client<C, I, L, P>(
            client: ClientRetryWrapper<C>,
            location: C::Location,
            part_requests: I,
            probe: P,
            log_dl_msg_dbg: L,
            download_span_guard: DownloadSpanGuard,
        ) -> Self
        where
            I: Iterator<Item = PartRequest> + Send + 'static,
            L: Into<LogDownloadMessagesAsDebug>,
            C: CondowClient,
            P: Probe + Clone,
        {
            let get_part_stream = {
                let probe = probe.clone();
                move |range: InclusiveRange| {
                    client
                        .download(location.clone(), range, probe.clone())
                        .boxed()
                }
            };

            Self::new(
                get_part_stream,
                part_requests,
                Arc::new(probe),
                log_dl_msg_dbg,
                download_span_guard,
            )
        }
    }

    impl Stream for DownloadPartsSeq {
        type Item = ChunkStreamItem;

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
                        Poll::Ready(Some(Ok(chunk))) => {
                            *this.state = State::Streaming(part_stream);
                            Poll::Ready(Some(Ok(chunk)))
                        }
                        Poll::Ready(Some(Err(err))) => {
                            this.probe
                                .download_failed(Some(this.download_started_at.elapsed()));
                            this.log_dl_msg_dbg.log(format!("download failed: {err}"));
                            *this.state = State::Finished;
                            Poll::Ready(Some(Err(err)))
                        }
                        Poll::Ready(None) => {
                            if let Some(part_request) = this.part_requests.next() {
                                let stream = PartChunksStream::new(
                                    this.get_part_stream,
                                    part_request,
                                    this.probe.clone(),
                                    this.download_span_guard.span(),
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

    #[cfg(test)]
    mod tests {
        use futures::StreamExt;

        use crate::{
            condow_client::{
                failing_client_simulator::FailingClientSimulatorBuilder, IgnoreLocation,
            },
            errors::{CondowError, CondowErrorKind},
            machinery::part_request::PartRequestIterator,
            retry::ClientRetryWrapper,
            streams::BytesHint,
            test_utils::TestCondowClient,
            ChunkStream,
        };

        use super::DownloadPartsSeq;

        #[tokio::test]
        async fn get_ranges() {
            let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
            for part_size in 1..100 {
                let part_requests = PartRequestIterator::new(0..=99, part_size);

                let poll_parts = DownloadPartsSeq::from_client(
                    client.clone(),
                    IgnoreLocation,
                    part_requests,
                    (),
                    true,
                    Default::default(),
                );

                let result = ChunkStream::from_stream(poll_parts.boxed(), BytesHint::new_no_hint())
                    .into_vec()
                    .await
                    .unwrap();

                let expected = &client.inner_client().data_slice()[0..=99];
                assert_eq!(result, expected, "part_size: {part_size}");
            }
        }

        #[tokio::test]
        async fn failures_with_retries() {
            let blob = (0u32..=999).map(|x| x as u8).collect::<Vec<_>>();

            let client = FailingClientSimulatorBuilder::default()
                .blob(blob.clone())
                .chunk_size(7)
                .responses()
                .success()
                .failure(CondowErrorKind::Io)
                .success()
                .success_with_stream_failure(3)
                .success()
                .failures([CondowErrorKind::Io, CondowErrorKind::Remote])
                .success_with_stream_failure(6)
                .failure(CondowError::new_remote("this did not work"))
                .success_with_stream_failure(2)
                .finish();

            let client = ClientRetryWrapper::new(client, Some(Default::default()));

            let part_requests = PartRequestIterator::new(0..=999, 13);

            let poll_parts = DownloadPartsSeq::from_client(
                client.clone(),
                IgnoreLocation,
                part_requests,
                (),
                true,
                Default::default(),
            );

            let result = ChunkStream::from_stream(poll_parts.boxed(), BytesHint::new_no_hint())
                .into_vec()
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected);
        }

        #[tokio::test]
        async fn pending_on_request() {
            let client = TestCondowClient::new().pending_on_request_n_times(1);
            let blob = client.data_slice().to_vec();
            let client = ClientRetryWrapper::new(client, Default::default());

            let part_requests = PartRequestIterator::new(..=(blob.len() as u64 - 1), 13);

            let poll_parts = DownloadPartsSeq::from_client(
                client.clone(),
                IgnoreLocation,
                part_requests,
                (),
                true,
                Default::default(),
            );

            let result = ChunkStream::from_stream(poll_parts.boxed(), BytesHint::new_no_hint())
                .into_vec()
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected);
        }

        #[tokio::test]
        async fn pending_on_stream() {
            let client = TestCondowClient::new().pending_on_stream_n_times(1);
            let blob = client.data_slice().to_vec();
            let client = ClientRetryWrapper::new(client, Default::default());

            let part_requests = PartRequestIterator::new(..=(blob.len() as u64 - 1), 13);

            let poll_parts = DownloadPartsSeq::from_client(
                client.clone(),
                IgnoreLocation,
                part_requests,
                (),
                true,
                Default::default(),
            );

            let result = ChunkStream::from_stream(poll_parts.boxed(), BytesHint::new_no_hint())
                .into_vec()
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected);
        }

        #[tokio::test]
        async fn pending_on_request_and_stream() {
            let client = TestCondowClient::new()
                .pending_on_request_n_times(1)
                .pending_on_stream_n_times(1);
            let blob = client.data_slice().to_vec();
            let client = ClientRetryWrapper::new(client, Default::default());

            let part_requests = PartRequestIterator::new(..=(blob.len() as u64 - 1), 13);

            let poll_parts = DownloadPartsSeq::from_client(
                client.clone(),
                IgnoreLocation,
                part_requests,
                (),
                true,
                Default::default(),
            );

            let result = ChunkStream::from_stream(poll_parts.boxed(), BytesHint::new_no_hint())
                .into_vec()
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected);
        }
    }
}
