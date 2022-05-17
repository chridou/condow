use std::{task::Poll, time::Instant};

use futures::{stream::BoxStream, Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::{
    condow_client::CondowClient,
    config::{Config, LogDownloadMessagesAsDebug},
    errors::CondowError,
    machinery::part_request::{PartRequest, PartRequestIterator},
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesHint, BytesStream, Chunk, ChunkStream, ChunkStreamItem},
    InclusiveRange,
};

mod yield_bytes_streams;

/// Download the parts sequentially.
///
/// The download is driven by the returned stream.
pub(crate) async fn download_chunks_sequentially<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
    config: Config,
) -> ChunkStream {
    probe.download_started();
    let bytes_hint = part_requests.bytes_hint();
    let poll_parts = PollPartsSeq::new(
        client,
        location,
        part_requests,
        probe,
        config.log_download_messages_as_debug,
    );

    let chunk_stream = ChunkStream::from_stream(poll_parts.boxed(), bytes_hint);

    chunk_stream
}

/// Stateful data for streaming a part
struct StreamingPart {
    bytes_stream: BytesStream,
    part_index: u64,
    chunk_index: usize,
    blob_offset: u64,
    range_offset: u64,
    bytes_left: u64,
    bytes_streams: BoxStream<'static, Result<(BytesStream, BytesHint, PartRequest), CondowError>>,
    part_started_at: Instant,
    part_range: InclusiveRange,
}

/// Internal state of the stream.
enum State {
    /// We are waiting for a new [BytesStream] alongside the corresponding [PartRequest].
    WaitingForStream {
        bytes_streams:
            BoxStream<'static, Result<(BytesStream, BytesHint, PartRequest), CondowError>>,
    },
    /// We are streming the [Chunk]s of a part.
    Streaming(StreamingPart),
    /// Nothing more to do. Always return `None`
    Finished,
}

pin_project! {
    /// A strem which polls parts of a download and yields the [Chunk]s in order
struct PollPartsSeq<P> {
    state: State,
    probe: P,
    download_started_at: Instant,
    log_dl_msg_dbg: LogDownloadMessagesAsDebug,
}
}

impl<P> PollPartsSeq<P>
where
    P: Probe + Clone,
{
    fn new<I, C, L>(
        client: ClientRetryWrapper<C>,
        location: C::Location,
        part_requests: I,
        probe: P,
        log_dl_msg_dbg: L,
    ) -> Self
    where
        I: Iterator<Item = PartRequest> + Send + 'static,
        C: CondowClient,
        L: Into<LogDownloadMessagesAsDebug>,
    {
        let bytes_streams = yield_bytes_streams::YieldBytesStreams::new(
            client,
            location,
            part_requests,
            probe.clone(),
        );

        Self {
            state: State::WaitingForStream {
                bytes_streams: bytes_streams.boxed(),
            },
            probe,
            download_started_at: Instant::now(),
            log_dl_msg_dbg: log_dl_msg_dbg.into(),
        }
    }
}

impl<P> Stream for PollPartsSeq<P>
where
    P: Probe + Clone,
{
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
            State::Finished => Poll::Ready(None),
            State::WaitingForStream { mut bytes_streams } => {
                match bytes_streams.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok((bytes_stream, _, part_request)))) => {
                        this.probe
                            .part_started(part_request.part_index, part_request.blob_range);

                        *this.state = State::Streaming(StreamingPart {
                            bytes_stream,
                            part_index: part_request.part_index,
                            chunk_index: 0,
                            blob_offset: part_request.blob_range.start(),
                            range_offset: part_request.range_offset,
                            bytes_left: part_request.blob_range.len(),
                            bytes_streams,
                            part_started_at: Instant::now(),
                            part_range: part_request.blob_range,
                        });
                        cx.waker().wake_by_ref(); // Stream sent "Ready" we return "Pending" -> We need to wake!
                        Poll::Pending
                    }
                    Poll::Ready(Some(Err(err))) => {
                        this.probe
                            .download_failed(Some(this.download_started_at.elapsed()));
                        this.log_dl_msg_dbg.log(format!("download failed: {err}"));
                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                    Poll::Ready(None) => {
                        this.log_dl_msg_dbg.log("download completed");
                        this.probe
                            .download_completed(this.download_started_at.elapsed());
                        *this.state = State::Finished;
                        Poll::Ready(None)
                    }
                    Poll::Pending => {
                        *this.state = State::WaitingForStream { bytes_streams };
                        Poll::Pending
                    }
                }
            }
            State::Streaming(mut streaming_state) => {
                match streaming_state.bytes_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        let bytes_len = bytes.len() as u64;
                        streaming_state.bytes_left -= bytes_len;
                        let chunk = Chunk {
                            part_index: streaming_state.part_index,
                            chunk_index: streaming_state.chunk_index,
                            blob_offset: streaming_state.blob_offset,
                            range_offset: streaming_state.range_offset,
                            bytes,
                            bytes_left: streaming_state.bytes_left,
                        };

                        this.probe.chunk_received(
                            streaming_state.part_index,
                            streaming_state.chunk_index,
                            bytes_len as usize,
                        );

                        streaming_state.chunk_index += 1;
                        streaming_state.blob_offset += bytes_len;
                        streaming_state.range_offset += bytes_len;
                        *this.state = State::Streaming(streaming_state);
                        Poll::Ready(Some(Ok(chunk)))
                    }
                    Poll::Ready(Some(Err(err))) => {
                        let err: CondowError = err.into();
                        this.probe.part_failed(
                            &err,
                            streaming_state.part_index,
                            &streaming_state.part_range,
                        );
                        this.probe
                            .download_failed(Some(this.download_started_at.elapsed()));
                        this.log_dl_msg_dbg.log(format!("download failed: {err}"));
                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                    Poll::Ready(None) => {
                        this.probe.part_completed(
                            streaming_state.part_index,
                            streaming_state.chunk_index,
                            streaming_state.part_range.len(),
                            streaming_state.part_started_at.elapsed(),
                        );
                        *this.state = State::WaitingForStream {
                            bytes_streams: streaming_state.bytes_streams,
                        };
                        cx.waker().wake_by_ref(); // Bytes Stream returned "Ready" and will not wake us up!
                        Poll::Pending
                    }
                    Poll::Pending => {
                        *this.state = State::Streaming(streaming_state);
                        Poll::Pending
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::{
        condow_client::{failing_client_simulator::FailingClientSimulatorBuilder, IgnoreLocation},
        errors::{CondowError, CondowErrorKind},
        machinery::part_request::PartRequestIterator,
        retry::ClientRetryWrapper,
        streams::BytesHint,
        test_utils::TestCondowClient,
        ChunkStream,
    };

    use super::PollPartsSeq;

    #[tokio::test]
    async fn get_ranges() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        for part_size in 1..100 {
            let part_requests = PartRequestIterator::new(0..=99, part_size);

            let poll_parts =
                PollPartsSeq::new(client.clone(), IgnoreLocation, part_requests, (), true);

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

        let poll_parts = PollPartsSeq::new(client.clone(), IgnoreLocation, part_requests, (), true);

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

        let poll_parts = PollPartsSeq::new(client.clone(), IgnoreLocation, part_requests, (), true);

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

        let poll_parts = PollPartsSeq::new(client.clone(), IgnoreLocation, part_requests, (), true);

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

        let poll_parts = PollPartsSeq::new(client.clone(), IgnoreLocation, part_requests, (), true);

        let result = ChunkStream::from_stream(poll_parts.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = blob;
        assert_eq!(result, expected);
    }
}
