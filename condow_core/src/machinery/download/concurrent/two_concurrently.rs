//! Download with a maximum concurrency of 2
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
    machinery::{download::PartChunksStream, part_request::PartRequest, DownloadSpanGuard},
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesHint, BytesStream, ChunkStreamItem},
    InclusiveRange,
};

pin_project! {
    /// Downloads pats with a maximum concurrency of 2.
    ///
    /// The download must be driven by polling the returned stream.
    ///
    /// The algorithm is "left biased" which means that it favors
    /// parts which have a lower part number (they come in ordered).
    ///
    /// This way there is less entropy in the ordering of the returned chunks.
    pub struct TwoPartsConcurrently<P: Probe> {
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
    download_span_guard: DownloadSpanGuard,
}

enum ActiveStreams<P: Probe> {
    /// Nothing more to do
    None,
    /// 2 or more parts left to download
    TwoConcurrently {
        left: PartChunksStream<P>,
        right: PartChunksStream<P>,
    },
    /// Exactly 1 part left to download
    LastPart(PartChunksStream<P>),
}

impl<P: Probe + Clone> TwoPartsConcurrently<P> {
    pub(crate) fn new<I, L, F>(
        get_part_stream: F,
        mut part_requests: I,
        probe: P,
        log_dl_msg_dbg: L,
        download_span_guard: DownloadSpanGuard,
    ) -> Self
    where
        I: Iterator<Item = PartRequest> + Send + 'static,
        L: Into<LogDownloadMessagesAsDebug>,
        F: Fn(InclusiveRange) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>
            + Send
            + 'static,
    {
        let log_dl_msg_dbg = log_dl_msg_dbg.into();

        probe.download_started();
        let active_streams = match (part_requests.next(), part_requests.next()) {
            (None, _) => {
                probe.download_completed(Duration::ZERO);

                log_dl_msg_dbg.log("download (empty) completed");

                ActiveStreams::None
            }
            (Some(first), None) => {
                let stream = PartChunksStream::new(
                    &get_part_stream,
                    first,
                    probe.clone(),
                    download_span_guard.span(),
                );
                ActiveStreams::LastPart(stream)
            }
            (Some(first), Some(second)) => {
                let left = PartChunksStream::new(
                    &get_part_stream,
                    first,
                    probe.clone(),
                    download_span_guard.span(),
                );
                let right = PartChunksStream::new(
                    &get_part_stream,
                    second,
                    probe.clone(),
                    download_span_guard.span(),
                );
                ActiveStreams::TwoConcurrently { left, right }
            }
        };

        let baggage = Baggage {
            get_part_stream: Box::new(get_part_stream),
            part_requests: Box::new(part_requests),
            probe,
            download_started_at: Instant::now(),
            log_dl_msg_dbg,
            download_span_guard,
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
        download_span_guard: DownloadSpanGuard,
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
            download_span_guard,
        )
    }
}

impl<P: Probe + Clone> Stream for TwoPartsConcurrently<P> {
    type Item = ChunkStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use Poll::*;

        let this = self.project();

        // We need to get ownership of the state. So we have to reassign it in each match
        // arm unless we want to be in "Finished" state.
        let active_streams = std::mem::replace(this.active_streams, ActiveStreams::None);

        match active_streams {
            ActiveStreams::None => Ready(None),
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
            ActiveStreams::TwoConcurrently { left, right } => {
                let (poll_result, next_state) = match poll_two(left, right, this.baggage, cx) {
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
        }
    }
}

/// poll "left biased" until there is only 1 part left
///
/// New parts are added "to the right"
fn poll_two<P: Probe + Clone>(
    mut left: PartChunksStream<P>,
    mut right: PartChunksStream<P>,
    baggage: &mut Baggage<P>,
    cx: &mut std::task::Context<'_>,
) -> Result<(Poll<Option<ChunkStreamItem>>, ActiveStreams<P>), CondowError> {
    match left.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(chunk))) => {
            return Ok((
                Poll::Ready(Some(Ok(chunk))),
                ActiveStreams::TwoConcurrently { left, right },
            ))
        }
        Poll::Ready(None) => {
            cx.waker().wake_by_ref();
            return if let Some(next_part_request) = baggage.part_requests.next() {
                let next_stream = PartChunksStream::new(
                    &baggage.get_part_stream,
                    next_part_request,
                    baggage.probe.clone(),
                    baggage.download_span_guard.span(),
                );
                Ok((
                    Poll::Pending,
                    ActiveStreams::TwoConcurrently {
                        left: right,
                        right: next_stream,
                    },
                ))
            } else {
                Ok((Poll::Pending, ActiveStreams::LastPart(right)))
            };
        }
        Poll::Ready(Some(Err(err))) => return Err(err),
        Poll::Pending => {} // nothing here? try the other one!
    };

    match right.poll_next_unpin(cx) {
        Poll::Ready(Some(Ok(chunk))) => Ok((
            Poll::Ready(Some(Ok(chunk))),
            ActiveStreams::TwoConcurrently { left, right },
        )),
        Poll::Ready(None) => {
            cx.waker().wake_by_ref();
            if let Some(next_part_request) = baggage.part_requests.next() {
                let next_stream = PartChunksStream::new(
                    &baggage.get_part_stream,
                    next_part_request,
                    baggage.probe.clone(),
                    baggage.download_span_guard.span(),
                );
                Ok((
                    Poll::Pending,
                    ActiveStreams::TwoConcurrently {
                        left,
                        right: next_stream,
                    },
                ))
            } else {
                Ok((Poll::Pending, ActiveStreams::LastPart(left)))
            }
        }
        Poll::Ready(Some(Err(err))) => Err(err),
        Poll::Pending => Ok((
            Poll::Pending,
            ActiveStreams::TwoConcurrently { left, right },
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

    use super::TwoPartsConcurrently;

    #[tokio::test]
    async fn empty() {
        let client = ClientRetryWrapper::new(TestCondowClient::new().max_jitter_ms(5), None);
        let part_requests = PartRequestIterator::empty();

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

            let stream = TwoPartsConcurrently::from_client(
                client.clone(),
                IgnoreLocation,
                part_requests,
                (),
                true,
                Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
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

        let stream = TwoPartsConcurrently::from_client(
            client.clone(),
            IgnoreLocation,
            part_requests,
            (),
            true,
            Default::default(),
        );

        let result = ChunkStream::from_stream(stream.boxed(), BytesHint::new_no_hint())
            .into_vec()
            .await
            .unwrap();

        let expected = blob;
        assert_eq!(result, expected);
    }
}
