use std::{sync::Arc, task::Poll};

use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use tracing::warn;

use crate::{
    condow_client::{ClientBytesStream, CondowClient},
    errors::CondowError,
    probe::Probe,
    streams::BytesStreamItem,
    InclusiveRange,
};

use super::RetryConfig;

type GetStreamFut = BoxFuture<'static, Result<ClientBytesStream, CondowError>>;

type GetStreamFn = Arc<dyn Fn(InclusiveRange) -> GetStreamFut + Send + Sync + 'static>;

enum RetryPartStreamState<P> {
    Streaming(RetryResumePartStream<P>),
    Finished,
}

pin_project! {
    pub struct RetryPartStream<P> {
        state: RetryPartStreamState<P>,
    }
}

impl<P> RetryPartStream<P>
where
    P: Probe,
{
    pub async fn new(
        get_stream_fn: GetStreamFn,
        initial_range: InclusiveRange,
        config: RetryConfig,
        probe: P,
    ) -> Result<Self, CondowError> {
        let probe = Arc::new(probe);
        let get_stream_fn = gen_retry_get_stream_fn(get_stream_fn, config, Arc::clone(&probe));

        let initial_stream = get_stream_fn(initial_range).await?;

        let resumable_stream =
            RetryResumePartStream::new(initial_range, initial_stream, get_stream_fn, config, probe);

        Ok(Self {
            state: RetryPartStreamState::Streaming(resumable_stream),
        })
    }

    pub async fn from_client<C: CondowClient>(
        client: &C,
        location: C::Location,
        initial_range: InclusiveRange,
        config: RetryConfig,
        probe: P,
    ) -> Result<Self, CondowError> {
        let get_part_stream = {
            let client = client.clone();
            move |range: InclusiveRange| client.download(location.clone(), range).boxed()
        };

        Self::new(Arc::new(get_part_stream), initial_range, config, probe).await
    }
}

impl<P> Stream for RetryPartStream<P>
where
    P: Probe,
{
    type Item = BytesStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        use RetryPartStreamState::*;

        let this = self.project();

        // We need to get ownership of the state. So we have to reassign it in each match
        // arm unless we want to be in "Finished" state.
        let state = std::mem::replace(this.state, Finished);

        match state {
            Streaming(mut resumable_stream) => match resumable_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    *this.state = Streaming(resumable_stream);
                    Poll::Ready(Some(Ok(bytes)))
                }
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => {
                    *this.state = Streaming(resumable_stream);
                    Poll::Pending
                }
            },
            Finished => Poll::Ready(None),
        }
    }
}

enum RetryResumePartStreamState {
    GettingStream(GetStreamFut, usize),
    StreamingAfterResume(ClientBytesStream, usize),
    Streaming(ClientBytesStream),
    Finished,
}

pin_project! {
    pub struct RetryResumePartStream<P> {
        get_stream_fn: GetStreamFn,
        config: RetryConfig,
        current_range: InclusiveRange,
        state: RetryResumePartStreamState,
        probe: Arc<P>,
        original_range: InclusiveRange,
    }
}

impl<P> RetryResumePartStream<P>
where
    P: Probe,
{
    pub fn new(
        initial_range: InclusiveRange,
        bytes_stream: ClientBytesStream,
        get_stream_fn: GetStreamFn,
        config: RetryConfig,
        probe: Arc<P>,
    ) -> Self {
        Self {
            current_range: initial_range,
            get_stream_fn,
            config,
            state: RetryResumePartStreamState::Streaming(bytes_stream),
            probe,
            original_range: initial_range,
        }
    }
}

impl<P> Stream for RetryResumePartStream<P>
where
    P: Probe,
{
    type Item = BytesStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        use RetryResumePartStreamState::*;

        let this = self.project();

        // We need to get ownership of the state. So we have to reassign it in each match
        // arm unless we want to be in "Finished" state.
        let state = std::mem::replace(this.state, Finished);

        match state {
            Streaming(mut bytes_stream) => match bytes_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    this.current_range.advance(bytes.len() as u64);
                    *this.state = Streaming(bytes_stream);
                    Poll::Ready(Some(Ok(bytes)))
                }
                Poll::Ready(Some(Err(err))) => {
                    if *this.config.max_stream_resume_attempts > 0 && err.is_retryable() {
                        warn!(
                            "streaming failed with error \"{err}\" - retrying on remaining \
                            range {}",
                            this.current_range
                        );
                        this.probe.stream_resume_attempt(
                            &err,
                            *this.original_range,
                            *this.current_range,
                        );

                        *this.state = GettingStream(
                            (this.get_stream_fn)(*this.current_range),
                            *this.config.max_stream_resume_attempts - 1,
                        );
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(Err(err)))
                    }
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => {
                    *this.state = Streaming(bytes_stream);
                    Poll::Pending
                }
            },
            StreamingAfterResume(mut bytes_stream, attempts_left) => {
                match bytes_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        this.current_range.advance(bytes.len() as u64);
                        *this.state = Streaming(bytes_stream);
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    Poll::Ready(Some(Err(err))) => {
                        if attempts_left > 0 && err.is_retryable() {
                            warn!(
                                "streaming failed with error \"{err}\" - retrying on remaining \
                            range {}",
                                this.current_range
                            );
                            this.probe.stream_resume_attempt(
                                &err,
                                *this.original_range,
                                *this.current_range,
                            );

                            *this.state = GettingStream(
                                (this.get_stream_fn)(*this.current_range),
                                attempts_left - 1,
                            );
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            Poll::Ready(Some(Err(err)))
                        }
                    }
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => {
                        *this.state = StreamingAfterResume(bytes_stream, attempts_left);
                        Poll::Pending
                    }
                }
            }
            GettingStream(mut get_stream_fut, attempts_left) => match get_stream_fut.poll_unpin(cx)
            {
                Poll::Ready(Ok(bytes_stream)) => {
                    *this.state = StreamingAfterResume(bytes_stream, attempts_left);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                Poll::Pending => {
                    *this.state = GettingStream(get_stream_fut, attempts_left);
                    Poll::Pending
                }
            },
            Finished => Poll::Ready(None),
        }
    }
}

pub(crate) fn gen_retry_get_stream_fn<P>(
    get_stream_fn_no_retries: GetStreamFn,
    config: RetryConfig,
    probe: Arc<P>,
) -> GetStreamFn
where
    P: Probe,
{
    let get_with_retries_fn: GetStreamFn = Arc::new(move |range: InclusiveRange| {
        let get_stream_fn_no_retries = Arc::clone(&get_stream_fn_no_retries);
        let probe = Arc::clone(&probe);
        async move {
            // The first attempt
            let mut last_err = match get_stream_fn_no_retries(range).await {
                Ok(bytes_stream) => return Ok(bytes_stream),
                Err(err) if err.is_retryable() => err,
                Err(err) => return Err(err),
            };

            // Retries if the first attempt failed
            let mut delays = config.iterator();
            for delay in delays.by_ref() {
                warn!("get stream request failed with \"{last_err}\" - retry in {delay:?}");
                probe.retry_attempt(&last_err, delay);

                tokio::time::sleep(delay).await;

                last_err = match get_stream_fn_no_retries(range).await {
                    Ok(stream_and_hint) => return Ok(stream_and_hint),
                    Err(err) if err.is_retryable() => err,
                    Err(err) => return Err(err),
                };
            }

            Err(last_err)
        }
        .boxed()
    });
    get_with_retries_fn
}
