//! A stream which queries a [CondowClient] for [BytesStream]s
//! from an iterator of [PartRequest]s.

use std::task::Poll;

use futures::{future::BoxFuture, FutureExt, Stream};
use pin_project_lite::pin_project;

use crate::{
    condow_client::CondowClient,
    errors::CondowError,
    machinery::part_request::PartRequest,
    probe::Probe,
    retry::ClientRetryWrapper,
    streams::{BytesHint, BytesStream},
};

enum State {
    /// Start state. We never enter this one again.
    Start,
    /// A future to yield a [BytesStream] was created. It needs to be polled
    /// until it retuns the stream.
    GettingStream {
        fut: BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>,
        part_request: PartRequest,
    },
    /// Nothing to do anymore. Always return `None`.
    Finished,
}

pin_project! {
    /// A stream which queries the client on `poll_next` for a [BytesStream]
    /// on a [PartRequest] as long as there are [PartRequest]s left
    pub struct YieldBytesStreams<C: CondowClient, P> {
        client: ClientRetryWrapper<C>,
        location: C::Location,
        part_requests: Box<dyn Iterator<Item=PartRequest>+ Send + 'static>,
        state: State,
        probe: P,
    }
}

impl<C, P> YieldBytesStreams<C, P>
where
    C: CondowClient,
    P: Probe + Clone,
{
    pub(crate) fn new<I>(
        client: ClientRetryWrapper<C>,
        location: C::Location,
        part_requests: I,
        probe: P,
    ) -> Self
    where
        I: Iterator<Item = PartRequest> + Send + 'static,
    {
        YieldBytesStreams {
            client,
            location,
            part_requests: Box::new(part_requests),
            state: State::Start,
            probe,
        }
    }
}

impl<C, P> Stream for YieldBytesStreams<C, P>
where
    C: CondowClient,
    P: Probe + Clone,
{
    type Item = Result<(BytesStream, BytesHint, PartRequest), CondowError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        // We need to get ownership of the state. So we have to reassign it in each match
        // arm unless we want to be in "Finished" state.
        let state = std::mem::replace(this.state, State::Finished);

        match state {
            State::Start => {
                if let Some(part_request) = this.part_requests.next() {
                    let fut = this
                        .client
                        .download(
                            this.location.clone(),
                            part_request.blob_range.into(),
                            this.probe.clone(),
                        )
                        .boxed();
                    *this.state = State::GettingStream { fut, part_request };
                    cx.waker().wake_by_ref(); // We send a "pending" even though we received a "ready"
                    Poll::Pending
                } else {
                    // No more requests so we are finished
                    *this.state = State::Finished;
                    Poll::Ready(None)
                }
            }
            State::GettingStream {
                mut fut,
                part_request,
            } => match fut.poll_unpin(cx) {
                Poll::Pending => {
                    *this.state = State::GettingStream { fut, part_request };
                    Poll::Pending
                } // Nothing there. Poll again later. Future will wake us up.
                Poll::Ready(Ok((stream, bytes_hint))) => {
                    *this.state = State::Start;
                    Poll::Ready(Some(Ok((stream, bytes_hint, part_request))))
                }
                Poll::Ready(Err(err)) => {
                    *this.state = State::Finished;
                    Poll::Ready(Some(Err(err)))
                }
            },
            State::Finished => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::{future, TryStreamExt};

    use crate::{
        condow_client::{failing_client_simulator::FailingClientSimulatorBuilder, IgnoreLocation},
        errors::{CondowError, CondowErrorKind},
        machinery::part_request::PartRequestIterator,
        retry::ClientRetryWrapper,
        test_utils::TestCondowClient,
    };

    use super::YieldBytesStreams;

    #[tokio::test]
    async fn non_pending_input_stream() {
        let client = TestCondowClient::new();
        let blob = client.data_slice().to_vec();
        let client = ClientRetryWrapper::new(client, Default::default());

        let part_requests = PartRequestIterator::new(..=(blob.len() as u64 - 1), 13);

        let stream = YieldBytesStreams::new(client, IgnoreLocation, part_requests, ());
        let stream = stream.map_ok(|(st, _, _)| st);
        let result = stream
            .try_fold(Vec::new(), |mut acc, bytes_stream| async move {
                bytes_stream
                    .try_for_each(|bytes: Bytes| {
                        acc.extend_from_slice(&bytes);
                        future::ok(())
                    })
                    .await?;
                Ok(acc)
            })
            .await
            .unwrap();

        let expected = blob;
        assert_eq!(result, expected);
    }

    #[tokio::test]
    async fn pending_input_stream() {
        for pendings in 1..=5 {
            let client = TestCondowClient::new().pending_on_stream_n_times(pendings);
            let blob = client.data_slice().to_vec();
            let client = ClientRetryWrapper::new(client, Default::default());

            let part_requests = PartRequestIterator::new(..=(blob.len() as u64 - 1), 13);

            let stream = YieldBytesStreams::new(client, IgnoreLocation, part_requests, ());
            let stream = stream.map_ok(|(st, _, _)| st);
            let result = stream
                .try_fold(Vec::new(), |mut acc, bytes_stream| async move {
                    bytes_stream
                        .try_for_each(|bytes: Bytes| {
                            acc.extend_from_slice(&bytes);
                            future::ok(())
                        })
                        .await?;
                    Ok(acc)
                })
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected, "pendings: {pendings}");
        }
    }

    #[tokio::test]
    async fn pending_on_request() {
        for pendings in 1..=5 {
            let client = TestCondowClient::new().pending_on_request_n_times(pendings);
            let blob = client.data_slice().to_vec();
            let client = ClientRetryWrapper::new(client, Default::default());

            let part_requests = PartRequestIterator::new(..=(blob.len() as u64 - 1), 13);

            let stream = YieldBytesStreams::new(client, IgnoreLocation, part_requests, ());
            let stream = stream.map_ok(|(st, _, _)| st);
            let result = stream
                .try_fold(Vec::new(), |mut acc, bytes_stream| async move {
                    bytes_stream
                        .try_for_each(|bytes: Bytes| {
                            acc.extend_from_slice(&bytes);
                            future::ok(())
                        })
                        .await?;
                    Ok(acc)
                })
                .await
                .unwrap();

            let expected = blob;
            assert_eq!(result, expected, "pendings: {pendings}");
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

        let stream = YieldBytesStreams::new(client, IgnoreLocation, part_requests, ());

        let stream = stream.map_ok(|(stream, _, _)| stream);

        let result = stream
            .try_fold(Vec::new(), |mut acc, bytes_stream| async move {
                bytes_stream
                    .try_for_each(|bytes: Bytes| {
                        acc.extend_from_slice(&bytes);
                        future::ok(())
                    })
                    .await?;
                Ok(acc)
            })
            .await
            .unwrap();

        let expected = blob;
        assert_eq!(result, expected);
    }
}
