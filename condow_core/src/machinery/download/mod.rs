//! Perform the actual download
//!
//! Downloads can be done concurrently or sequentially.

use futures::{Stream, StreamExt};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::{
    config::{Config, LogDownloadMessagesAsDebug},
    errors::CondowError,
    probe::Probe,
    streams::ChunkStreamItem,
};

pub(crate) use concurrent::download_concurrently;
pub(crate) use part_chunks_stream::PartChunksStream;
pub(crate) use sequential::download_sequentially;

mod concurrent;
mod sequential;

pub fn active_pull<St, P: Probe>(
    mut input: St,
    probe: P,
    config: Config,
) -> mpsc::UnboundedReceiver<St::Item>
where
    St: Stream<Item = ChunkStreamItem> + Send + 'static + Unpin,
{
    let (sender, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let panic_guard = PanicGuard {
            sender: sender.clone(),
            probe: Box::new(probe),
            log_dl_msg_dbg: config.log_download_messages_as_debug,
        };

        while let Some(message) = input.next().await {
            if let Err(_err) = sender.send(message) {
                break;
            }
        }

        drop(panic_guard);
    });

    rx
}

struct PanicGuard<T> {
    sender: UnboundedSender<Result<T, CondowError>>,
    probe: Box<dyn Probe>,
    log_dl_msg_dbg: LogDownloadMessagesAsDebug,
}

impl<T> Drop for PanicGuard<T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // We need to send a finalizing error so that consumers know something bad happened
            let _ = self.sender.send(Err(CondowError::new_other(
                "download ended unexpectedly due to a panic",
            )));
            self.probe.download_failed(None);
            self.log_dl_msg_dbg
                .log("download failed due to a panic. check logs");
        }
    }
}

pub mod part_chunks_stream {
    use std::{task::Poll, time::Instant};

    use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
    use pin_project_lite::pin_project;

    use crate::{
        condow_client::CondowClient,
        errors::CondowError,
        machinery::part_request::PartRequest,
        probe::Probe,
        retry::ClientRetryWrapper,
        streams::{BytesHint, BytesStream, Chunk, ChunkStreamItem},
        InclusiveRange,
    };

    pin_project! {
        /// A stream which returns [ChunkStreamItem]s for a [PartRequest].
        ///
        /// It does a request via the client and returns the chunks of that request's
        /// [ByteStream]
        ///
        /// The supplied [Probe] is notified on part events and chunk events.
        /// Global download events are not published.
        pub struct PartChunksStream<P: Probe> {
            part_request: PartRequest,
            state: State,
            probe: P,
            started_at: Instant,
        }
    }

    impl<P> PartChunksStream<P>
    where
        P: Probe + Clone,
    {
        #[allow(dead_code)]
        pub(crate) fn from_client<C: CondowClient>(
            client: &ClientRetryWrapper<C>,
            location: C::Location,
            part_request: PartRequest,
            probe: P,
        ) -> Self {
            let get_part_stream = {
                let probe = probe.clone();
                move |range: InclusiveRange| {
                    client
                        .download(location.clone(), range.into(), probe.clone())
                        .boxed()
                }
            };

            Self::new(&get_part_stream, part_request, probe)
        }

        pub(crate) fn new(
            get_part_stream: &dyn Fn(
                InclusiveRange,
            ) -> BoxFuture<
                'static,
                Result<(BytesStream, BytesHint), CondowError>,
            >,
            part_request: PartRequest,
            probe: P,
        ) -> Self {
            let range = part_request.blob_range;
            PartChunksStream {
                part_request,
                state: State::GettingStream(get_part_stream(range)),
                probe,
                started_at: Instant::now(),
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
        GettingStream(BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>),
        Streaming(StreamingPart),
        /// Nothing to do anymore. Always return `None`.
        Finished,
    }

    impl<P> Stream for PartChunksStream<P>
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
                State::GettingStream(mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        *this.state = State::GettingStream(fut);
                        Poll::Pending
                    } // Nothing there. Poll again later. Future will wake us up.
                    Poll::Ready(Ok((bytes_stream, _bytes_hint))) => {
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
                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                },
                State::Streaming(mut streaming_state) => {
                    match streaming_state.bytes_stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(bytes))) => {
                            let bytes_len = bytes.len() as u64;
                            streaming_state.bytes_left -= bytes_len;

                            let chunk = Chunk {
                                part_index: this.part_request.part_index,
                                chunk_index: streaming_state.chunk_index,
                                blob_offset: streaming_state.blob_offset,
                                range_offset: streaming_state.range_offset,
                                bytes,
                                bytes_left: streaming_state.bytes_left,
                            };

                            this.probe.chunk_received(
                                chunk.part_index,
                                chunk.chunk_index,
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
                                this.part_request.part_index,
                                &this.part_request.blob_range,
                            );

                            *this.state = State::Finished;
                            Poll::Ready(Some(Err(err)))
                        }
                        Poll::Ready(None) => {
                            this.probe.part_completed(
                                this.part_request.part_index,
                                streaming_state.chunk_index,
                                this.part_request.blob_range.len(),
                                this.started_at.elapsed(),
                            );
                            *this.state = State::Finished;
                            Poll::Ready(None)
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

    #[cfg(test)]
    mod tests {
        use futures::StreamExt;

        use crate::{
            condow_client::{
                failing_client_simulator::FailingClientSimulatorBuilder, IgnoreLocation,
            },
            errors::{CondowError, CondowErrorKind},
            machinery::part_request::PartRequest,
            retry::ClientRetryWrapper,
            streams::BytesHint,
            test_utils::TestCondowClient,
            ChunkStream,
        };

        use super::PartChunksStream;

        #[tokio::test]
        async fn get_ranges() {
            let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
            let part_request = PartRequest {
                part_index: 0,
                blob_range: (0..=99).into(),
                range_offset: 0,
            };

            let chunks = PartChunksStream::from_client(&client, IgnoreLocation, part_request, ());

            let result = ChunkStream::from_stream(chunks.boxed(), BytesHint::new_no_hint())
                .into_vec()
                .await
                .unwrap();

            let expected = &client.inner_client().data_slice()[0..=99];
            assert_eq!(result, expected);
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

            let part_request = PartRequest {
                part_index: 0,
                blob_range: (0..=999).into(),
                range_offset: 0,
            };
            let chunks = PartChunksStream::from_client(&client, IgnoreLocation, part_request, ());

            let result = ChunkStream::from_stream(chunks.boxed(), BytesHint::new_no_hint())
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

            let part_request = PartRequest {
                part_index: 0,
                blob_range: (0..=(blob.len() as u64 - 1)).into(),
                range_offset: 0,
            };
            let chunks = PartChunksStream::from_client(&client, IgnoreLocation, part_request, ());

            let result = ChunkStream::from_stream(chunks.boxed(), BytesHint::new_no_hint())
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

            let part_request = PartRequest {
                part_index: 0,
                blob_range: (0..=(blob.len() as u64 - 1)).into(),
                range_offset: 0,
            };
            let chunks = PartChunksStream::from_client(&client, IgnoreLocation, part_request, ());

            let result = ChunkStream::from_stream(chunks.boxed(), BytesHint::new_no_hint())
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

            let part_request = PartRequest {
                part_index: 0,
                blob_range: (0..=(blob.len() as u64 - 1)).into(),
                range_offset: 0,
            };
            let chunks = PartChunksStream::from_client(&client, IgnoreLocation, part_request, ());

            let result = ChunkStream::from_stream(chunks.boxed(), BytesHint::new_no_hint())
                .into_vec()
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected);
        }
    }
}
