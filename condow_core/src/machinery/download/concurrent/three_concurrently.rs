use std::{
    task::Poll,
    time::{Duration, Instant},
};

use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::{
    condow_client::CondowClient,
    config::LogDownloadMessagesAsDebug,
    errors::CondowError,
    machinery::{download::PartChunksStream, part_request::PartRequest},
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesHint, BytesStream, ChunkStreamItem},
    InclusiveRange,
};

pin_project! {
    pub struct ThreePartsConcurrently<P: Probe> {
        active_streams: ActiveStreams<P>,
        baggage: Baggage<P>,
    }
}

struct Baggage<P: Probe> {
    get_part_stream: Box<
        dyn Fn(InclusiveRange) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>
            + Send
            + 'static,
    >,
    part_requests: Box<dyn Iterator<Item = PartRequest> + Send + 'static>,
    probe: P,
    download_started_at: Instant,
    log_dl_msg_dbg: LogDownloadMessagesAsDebug,
}

enum ActiveStreams<P: Probe> {
    None,
    ThreeConcurrently {
        left: PartChunksStream<P>,
        middle: PartChunksStream<P>,
        right: PartChunksStream<P>,
    },
    LastTwoConcurrently {
        left: PartChunksStream<P>,
        right: PartChunksStream<P>,
    },
    LastPart(PartChunksStream<P>),
}

impl<P: Probe + Clone> ThreePartsConcurrently<P> {
    pub fn new<I, L, F>(
        get_part_stream: F,
        mut part_requests: I,
        probe: P,
        log_dl_msg_dbg: L,
    ) -> Self
    where
        I: Iterator<Item = PartRequest> + Send + 'static,
        L: Into<LogDownloadMessagesAsDebug>,
        F: Fn(InclusiveRange) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>
            + Send
            + 'static,
    {
        probe.download_started();

        let log_dl_msg_dbg = log_dl_msg_dbg.into();

        let active_streams = match (
            part_requests.next(),
            part_requests.next(),
            part_requests.next(),
        ) {
            (None, _, _) => {
                probe.download_completed(Duration::ZERO);

                log_dl_msg_dbg.log("download (empty) completed");

                ActiveStreams::None
            }
            (Some(first), None, _) => {
                let stream = PartChunksStream::new(&get_part_stream, first, probe.clone());
                ActiveStreams::LastPart(stream)
            }
            (Some(first), Some(second), None) => {
                let left = PartChunksStream::new(&get_part_stream, first, probe.clone());
                let right = PartChunksStream::new(&get_part_stream, second, probe.clone());
                ActiveStreams::LastTwoConcurrently { left, right }
            }
            (Some(first), Some(second), Some(third)) => {
                let left = PartChunksStream::new(&get_part_stream, first, probe.clone());
                let middle = PartChunksStream::new(&get_part_stream, second, probe.clone());
                let right = PartChunksStream::new(&get_part_stream, third, probe.clone());
                ActiveStreams::ThreeConcurrently {
                    left,
                    middle,
                    right,
                }
            }
        };

        let baggage = Baggage {
            get_part_stream: Box::new(get_part_stream),
            part_requests: Box::new(part_requests),
            probe,
            download_started_at: Instant::now(),
            log_dl_msg_dbg,
        };

        Self {
            active_streams,
            baggage,
        }
    }

    pub(crate) fn from_client<C, I, L>(
        client: ClientRetryWrapper<C>,
        location: C::Location,
        part_requests: I,
        probe: P,
        log_dl_msg_dbg: L,
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

        Self::new(get_part_stream, part_requests, probe, log_dl_msg_dbg)
    }
}

impl<P: Probe + Clone> Stream for ThreePartsConcurrently<P> {
    type Item = ChunkStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use Poll::*;

        let mut this = self.project();

        // We need to get ownership of the state. So we have to reassign it in each match
        // arm unless we want to be in "Finished" state.
        let active_streams = std::mem::replace(this.active_streams, ActiveStreams::None);

        match active_streams {
            ActiveStreams::None => Ready(None),
            ActiveStreams::ThreeConcurrently {
                left,
                middle,
                right,
            } => {
                let (poll_result, next_state) =
                    match poll_three(left, middle, right, &mut this.baggage, cx) {
                        Ok(ok) => ok,
                        Err(err) => {
                            this.baggage
                                .probe
                                .download_failed(Some(this.baggage.download_started_at.elapsed()));
                            this.baggage.log_dl_msg_dbg.log("download failed: {err}");

                            return Ready(Some(Err(err)));
                        }
                    };
                *this.active_streams = next_state;

                poll_result
            }
            ActiveStreams::LastTwoConcurrently { left, right } => {
                let (poll_result, next_state) = match poll_last_two(left, right, cx) {
                    Ok(ok) => ok,
                    Err(err) => {
                        this.baggage
                            .probe
                            .download_failed(Some(this.baggage.download_started_at.elapsed()));
                        this.baggage.log_dl_msg_dbg.log("download failed: {err}");

                        return Ready(Some(Err(err)));
                    }
                };
                *this.active_streams = next_state;

                poll_result
            }
            ActiveStreams::LastPart(mut stream) => match stream.poll_next_unpin(cx) {
                Ready(Some(Ok(chunk))) => {
                    *this.active_streams = ActiveStreams::LastPart(stream);
                    Ready(Some(Ok(chunk)))
                }
                Ready(Some(Err(err))) => {
                    this.baggage
                        .probe
                        .download_failed(Some(this.baggage.download_started_at.elapsed()));
                    this.baggage.log_dl_msg_dbg.log("download failed: {err}");
                    Ready(Some(Err(err)))
                }
                Ready(None) => {
                    this.baggage
                        .probe
                        .download_completed(this.baggage.download_started_at.elapsed());
                    this.baggage.log_dl_msg_dbg.log("download completed");

                    Ready(None)
                }
                Pending => {
                    *this.active_streams = ActiveStreams::LastPart(stream);
                    Pending
                }
            },
        }
    }
}

fn poll_three<P: Probe + Clone>(
    mut left: PartChunksStream<P>,
    mut middle: PartChunksStream<P>,
    mut right: PartChunksStream<P>,
    baggage: &mut Baggage<P>,
    cx: &mut std::task::Context<'_>,
) -> Result<(Poll<Option<ChunkStreamItem>>, ActiveStreams<P>), CondowError> {
    match left.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(chunk))) => {
            return Ok((
                Poll::Ready(Some(Ok(chunk))),
                ActiveStreams::ThreeConcurrently {
                    left,
                    middle,
                    right,
                },
            ))
        }
        Poll::Ready(None) => {
            cx.waker().wake_by_ref();
            return if let Some(next_part_request) = baggage.part_requests.next() {
                let next_stream = PartChunksStream::new(
                    &baggage.get_part_stream,
                    next_part_request,
                    baggage.probe.clone(),
                );
                Ok((
                    Poll::Pending,
                    ActiveStreams::ThreeConcurrently {
                        left: middle,
                        middle: right,
                        right: next_stream,
                    },
                ))
            } else {
                Ok((
                    Poll::Pending,
                    ActiveStreams::LastTwoConcurrently {
                        left: middle,
                        right,
                    },
                ))
            };
        }
        Poll::Ready(Some(Err(err))) => return Err(err),
        Poll::Pending => {}
    };

    match middle.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(chunk))) => {
            return Ok((
                Poll::Ready(Some(Ok(chunk))),
                ActiveStreams::ThreeConcurrently {
                    left,
                    middle,
                    right,
                },
            ))
        }
        Poll::Ready(None) => {
            cx.waker().wake_by_ref();
            return if let Some(next_part_request) = baggage.part_requests.next() {
                let next_stream = PartChunksStream::new(
                    &baggage.get_part_stream,
                    next_part_request,
                    baggage.probe.clone(),
                );
                Ok((
                    Poll::Pending,
                    ActiveStreams::ThreeConcurrently {
                        left,
                        middle: right,
                        right: next_stream,
                    },
                ))
            } else {
                Ok((
                    Poll::Pending,
                    ActiveStreams::LastTwoConcurrently { left, right },
                ))
            };
        }
        Poll::Ready(Some(Err(err))) => return Err(err),
        Poll::Pending => {}
    }

    match right.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(chunk))) => Ok((
            Poll::Ready(Some(Ok(chunk))),
            ActiveStreams::ThreeConcurrently {
                left,
                middle,
                right,
            },
        )),
        Poll::Ready(None) => {
            cx.waker().wake_by_ref();
            if let Some(next_part_request) = baggage.part_requests.next() {
                let next_stream = PartChunksStream::new(
                    &baggage.get_part_stream,
                    next_part_request,
                    baggage.probe.clone(),
                );
                Ok((
                    Poll::Pending,
                    ActiveStreams::ThreeConcurrently {
                        left,
                        middle,
                        right: next_stream,
                    },
                ))
            } else {
                Ok((
                    Poll::Pending,
                    ActiveStreams::LastTwoConcurrently {
                        left,
                        right: middle,
                    },
                ))
            }
        }
        Poll::Ready(Some(Err(err))) => Err(err),
        Poll::Pending => Ok((
            Poll::Pending,
            ActiveStreams::ThreeConcurrently {
                left,
                middle,
                right,
            },
        )),
    }
}

fn poll_last_two<P: Probe + Clone>(
    mut left: PartChunksStream<P>,
    mut right: PartChunksStream<P>,
    cx: &mut std::task::Context<'_>,
) -> Result<(Poll<Option<ChunkStreamItem>>, ActiveStreams<P>), CondowError> {
    match left.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(chunk))) => {
            return Ok((
                Poll::Ready(Some(Ok(chunk))),
                ActiveStreams::LastTwoConcurrently { left, right },
            ))
        }
        Poll::Ready(None) => {
            cx.waker().wake_by_ref();
            return Ok((Poll::Pending, ActiveStreams::LastPart(right)));
        }
        Poll::Ready(Some(Err(err))) => return Err(err),
        Poll::Pending => {}
    };

    match right.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(chunk))) => Ok((
            Poll::Ready(Some(Ok(chunk))),
            ActiveStreams::LastTwoConcurrently { left, right },
        )),
        Poll::Ready(None) => {
            cx.waker().wake_by_ref();
            Ok((Poll::Pending, ActiveStreams::LastPart(left)))
        }
        Poll::Ready(Some(Err(err))) => Err(err),
        Poll::Pending => Ok((
            Poll::Pending,
            ActiveStreams::LastTwoConcurrently { left, right },
        )),
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

    use super::ThreePartsConcurrently;

    #[tokio::test]
    async fn empty() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        let part_requests = PartRequestIterator::empty();

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = &[];
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn one_part() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        let part_requests = PartRequestIterator::new(0..=99, 100);

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = &client.inner_client().data_slice()[0..=99];
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn two_parts() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        let part_requests = PartRequestIterator::new(0..=99, 50);

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = &client.inner_client().data_slice()[0..=99];
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn three_parts() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        let part_requests = PartRequestIterator::new(0..=99, 40);

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = &client.inner_client().data_slice()[0..=99];
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn four_parts() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        let part_requests = PartRequestIterator::new(0..=99, 25);

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = &client.inner_client().data_slice()[0..=99];
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn five_parts() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        let part_requests = PartRequestIterator::new(0..=99, 20);

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = &client.inner_client().data_slice()[0..=99];
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn get_ranges() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        for part_size in 1..=101 {
            let part_requests = PartRequestIterator::new(0..=99, part_size);

            let stream = ThreePartsConcurrently::from_client(
                client.clone(),
                IgnoreLocation,
                part_requests,
                (),
                true,
            );

            let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
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

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
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

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
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

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
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

        let stream = ThreePartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = blob;
        assert_eq!(result, expected);
    }
}
