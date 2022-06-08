//! Perform the actual download
//!
//! Downloads can be done concurrently or sequentially.

use std::{collections::VecDeque, task::Poll};

use futures::{ready, Stream, StreamExt};
use pin_project_lite::pin_project;
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::{config::LogDownloadMessagesAsDebug, errors::CondowError, probe::Probe};

pub(crate) use concurrent::{
    download_bytes_concurrently, download_chunks_concurrently, FourPartsConcurrently,
    ThreePartsConcurrently, TwoPartsConcurrently,
};
pub(crate) use part_chunks_stream::PartChunksStream;
pub(crate) use sequential::{
    download_bytes_sequentially, download_chunks_sequentially, DownloadPartsSeq, PartsBytesStream,
};

mod concurrent;
mod sequential;

/// This functions polls the given stream eagerly on a seperate task.
///
/// There is an infinite buffer in place.
///
/// The returned receiver can be used to retrieve the items.
///
/// This functions emits an error over the receiber in case a panic occurred whie
/// polling the input stream.
pub fn active_pull<St, T, P: Probe>(
    mut input: St,
    probe: P,
    log_dl_msg_dbg: LogDownloadMessagesAsDebug,
) -> ActiveStream<T>
where
    St: Stream<Item = Result<T, CondowError>> + Send + 'static + Unpin,
    T: Send + 'static,
{
    const CAPACITY: usize = 16;

    let (sender, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let panic_guard = PanicGuard {
            sender: sender.clone(),
            probe: Box::new(probe),
            log_dl_msg_dbg,
        };

        let mut buffer = VecDeque::with_capacity(CAPACITY);

        while let Some(message) = input.next().await {
            let payload = match message {
                Ok(payload) => payload,
                Err(err) => {
                    let _ = sender.send(Err(err));
                    return;
                }
            };

            if buffer.len() == buffer.capacity() {
                let _ = sender.send(Ok(buffer));
                buffer = VecDeque::with_capacity(CAPACITY);
            }

            buffer.push_back(payload);
        }

        if !buffer.is_empty() {
            let _ = sender.send(Ok(buffer));
        }

        drop(panic_guard);
    });

    ActiveStream::new(rx)
}

pin_project! {
pub struct ActiveStream<T> {
    #[pin]
    receiver: mpsc::UnboundedReceiver<Result<VecDeque<T>, CondowError>>,
    next: Option<VecDeque<T>>
}
}

impl<T> ActiveStream<T> {
    pub(crate) fn new(receiver: mpsc::UnboundedReceiver<Result<VecDeque<T>, CondowError>>) -> Self {
        Self {
            receiver,
            next: None,
        }
    }
}

impl<T> Stream for ActiveStream<T> {
    type Item = Result<T, CondowError>;

    #[inline]
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        Poll::Ready(loop {
            if let Some(queue) = this.next.as_mut() {
                if let Some(item) = queue.pop_front() {
                    break Some(Ok(item));
                } else {
                    *this.next = None;
                }
            } else if let Some(queue) = ready!(this.receiver.as_mut().poll_recv(cx)?) {
                *this.next = Some(queue);
            } else {
                break None;
            }
        })
    }
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
    use std::{sync::Arc, task::Poll, time::Instant};

    use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
    use pin_project_lite::pin_project;
    use tracing::{debug_span, Span};

    use crate::{
        condow_client::{ClientBytesStream, CondowClient},
        errors::CondowError,
        machinery::part_request::PartRequest,
        probe::Probe,
        retry::ClientRetryWrapper,
        streams::{Chunk, ChunkStreamItem},
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
        pub struct PartChunksStream {
            part_request: PartRequest,
            state: State,
            probe: Arc<dyn Probe>,
            started_at: Instant,
            part_span: Span,
        }
    }

    impl PartChunksStream {
        #[allow(dead_code)]
        pub(crate) fn from_client<C: CondowClient, P>(
            client: &ClientRetryWrapper<C>,
            location: C::Location,
            part_request: PartRequest,
            probe: P,
            parent: &Span,
        ) -> Self
        where
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

            Self::new(&get_part_stream, part_request, Arc::new(probe), parent)
        }

        pub(crate) fn new(
            get_part_stream: &dyn Fn(
                InclusiveRange,
            )
                -> BoxFuture<'static, Result<ClientBytesStream, CondowError>>,
            part_request: PartRequest,
            probe: Arc<dyn Probe>,
            parent: &Span,
        ) -> Self {
            let range = part_request.blob_range;
            probe.part_started(part_request.part_index, range);

            let part_span = debug_span!(parent: parent,
                "download_part", 
                part_index = %part_request.part_index,
                part_range = %part_request.blob_range,
                part_offset = %part_request.range_offset);

            PartChunksStream {
                part_request,
                state: State::GettingStream(get_part_stream(range)),
                probe,
                started_at: Instant::now(),
                part_span,
            }
        }
    }

    struct StreamingPart {
        bytes_stream: ClientBytesStream,
        chunk_index: usize,
        blob_offset: u64,
        range_offset: u64,
        bytes_left: u64,
    }

    enum State {
        /// A future to yield a [BytesStream] was created. It needs to be polled
        /// until it retuns the stream.
        GettingStream(BoxFuture<'static, Result<ClientBytesStream, CondowError>>),
        Streaming(StreamingPart),
        /// Nothing to do anymore. Always return `None`.
        Finished,
    }

    impl Stream for PartChunksStream {
        type Item = ChunkStreamItem;

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

                            let chunk = Chunk {
                                part_index: this.part_request.part_index,
                                chunk_index: streaming_state.chunk_index,
                                blob_offset: streaming_state.blob_offset,
                                range_offset: streaming_state.range_offset,
                                bytes,
                                bytes_left: streaming_state.bytes_left,
                            };

                            streaming_state.chunk_index += 1;
                            streaming_state.blob_offset += bytes_len;
                            streaming_state.range_offset += bytes_len;

                            *this.state = State::Streaming(streaming_state);
                            Poll::Ready(Some(Ok(chunk)))
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

    #[cfg(test)]
    mod tests {
        use futures::StreamExt;
        use tracing::Span;

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

            let chunks = PartChunksStream::from_client(
                &client,
                IgnoreLocation,
                part_request,
                (),
                &Span::none(),
            );

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
            let chunks = PartChunksStream::from_client(
                &client,
                IgnoreLocation,
                part_request,
                (),
                &Span::none(),
            );

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
            let chunks = PartChunksStream::from_client(
                &client,
                IgnoreLocation,
                part_request,
                (),
                &Span::none(),
            );

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
            let chunks = PartChunksStream::from_client(
                &client,
                IgnoreLocation,
                part_request,
                (),
                &Span::none(),
            );

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
            let chunks = PartChunksStream::from_client(
                &client,
                IgnoreLocation,
                part_request,
                (),
                &Span::none(),
            );

            let result = ChunkStream::from_stream(chunks.boxed(), BytesHint::new_no_hint())
                .into_vec()
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected);
        }
    }
}
