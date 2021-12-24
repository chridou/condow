use std::{sync::Arc, time::Duration};

use anyhow::Error as AnyError;
use bytes::Bytes;
use futures::{channel::mpsc, Future, Stream, StreamExt, TryFutureExt};

use crate::{
    condow_client::{CondowClient, DownloadSpec},
    errors::{CondowError, IoError},
    reporter::Reporter,
    streams::{BytesHint, BytesStream},
    InclusiveRange,
};

new_type! {
    #[doc="The delay for the first retry attempt in ms."]
    #[doc="Default is 50ms."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub copy struct RetryInitialDelayMs(u64, env="RETRY_INITIAL_DELAY_MS");
}

impl Default for RetryInitialDelayMs {
    fn default() -> Self {
        Self(50)
    }
}

impl From<RetryInitialDelayMs> for Duration {
    fn from(delay: RetryInitialDelayMs) -> Duration {
        Duration::from_millis(delay.0)
    }
}

new_type! {
    #[doc="The factor the previous retry is multiplied by."]
    #[doc="Default is 1.5."]
    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub copy struct RetryDelayFactor(f64, env="RETRY_DELAY_FACTOR");
}

impl Default for RetryDelayFactor {
    fn default() -> Self {
        Self(1.5)
    }
}

new_type! {
    #[doc="The maximum retry for a retry attempt."]
    #[doc="Default 5 seconds."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub copy struct RetryDelayMaxMs(u64, env="RETRY_DELAY_MAX_MS");
}

impl Default for RetryDelayMaxMs {
    fn default() -> Self {
        Self(5_000)
    }
}

impl From<RetryDelayMaxMs> for Duration {
    fn from(delay: RetryDelayMaxMs) -> Duration {
        Duration::from_millis(delay.0)
    }
}

new_type! {
    #[doc="The maximum number of retry attempts."]
    #[doc="This excludes the original attempt."]
    #[doc="Default is 2."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub copy struct RetryMaxAttempts(usize, env="RETRY_MAX_ATTEMPTS");
}

impl Default for RetryMaxAttempts {
    fn default() -> Self {
        Self(2)
    }
}

/// Configures retries with exponential backoff
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct RetryConfig {
    /// The maximum number of retry attempts.
    pub max_attempts: RetryMaxAttempts,
    /// The delay before the first retry attempt.
    pub initial_delay: RetryInitialDelayMs,
    /// A factor to multiply a current delay with to get the next one
    pub delay_factor: RetryDelayFactor,
    /// The maximum delay for a retry.
    pub max_delay: RetryDelayMaxMs,
    // TODO: Add possibility to jitter
}

impl RetryConfig {
    env_ctors!(no_fill);

    pub(crate) fn iterator(&self) -> impl Iterator<Item = Duration> {
        RetryIterator {
            attempts_left: self.max_attempts.into_inner(),
            next_delay_secs: self.initial_delay.into_inner() as f64 / 1_000.0,
            max_delay_secs: self.max_delay.into_inner() as f64 / 1_000.0,
            delay_factor: self.delay_factor.into_inner(),
        }
    }

    fn fill_from_env_prefixed_internal<T: AsRef<str>>(
        &mut self,
        prefix: T,
    ) -> Result<(), AnyError> {
        if let Some(max_attempts) = RetryMaxAttempts::try_from_env_prefixed(prefix.as_ref())? {
            self.max_attempts = max_attempts;
        }
        if let Some(initial_delay) = RetryInitialDelayMs::try_from_env_prefixed(prefix.as_ref())? {
            self.initial_delay = initial_delay;
        }
        if let Some(delay_factor) = RetryDelayFactor::try_from_env_prefixed(prefix.as_ref())? {
            self.delay_factor = delay_factor;
        }
        if let Some(max_delay) = RetryDelayMaxMs::try_from_env_prefixed(prefix.as_ref())? {
            self.max_delay = max_delay;
        }

        Ok(())
    }
}

pub struct RetryIterator {
    attempts_left: usize,
    next_delay_secs: f64,
    max_delay_secs: f64,
    delay_factor: f64,
}

impl Iterator for RetryIterator {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.attempts_left == 0 {
            return None;
        }

        self.attempts_left -= 1;

        let current_delay = Duration::from_secs_f64(self.next_delay_secs);

        self.next_delay_secs = (self.next_delay_secs * self.delay_factor).min(self.max_delay_secs);

        Some(current_delay)
    }
}

#[derive(Clone)]
pub struct ClientRetryWrapper<C> {
    inner: Arc<(C, Option<RetryConfig>)>,
}

impl<C> ClientRetryWrapper<C>
where
    C: CondowClient,
{
    pub async fn get_size<R: Reporter>(
        &self,
        location: C::Location,
        reporter: &R,
    ) -> Result<u64, CondowError>
    where
        R: Reporter,
    {
        let (client, config) = self.inner.as_ref();
        if let Some(config) = config {
            retry_get_size(client, location, config, reporter).await
        } else {
            Ok(client.get_size(location).await?)
        }
    }

    pub async fn download<R: Reporter>(
        &self,
        location: C::Location,
        spec: DownloadSpec,
        reporter: &R,
    ) -> Result<(BytesStream, BytesHint), CondowError> {
        let (client, config) = self.inner.as_ref();
        if let Some(config) = config {
            retry_download(client, location, spec, config, reporter).await
        } else {
            Ok(client.download(location, spec).await?)
        }
    }
}

/// Retries on the `get_size` request according to the [RetryConfig]
async fn retry_get_size<C, R>(
    client: &C,
    location: C::Location,
    config: &RetryConfig,
    reporter: &R,
) -> Result<u64, CondowError>
where
    C: CondowClient,
    R: Reporter,
{
    // The first attempt
    let mut last_err = match client.get_size(location.clone()).await {
        Ok(v) => return Ok(v),
        Err(err) if err.is_retryable() => err,
        Err(err) => return Err(err),
    };

    // Retries if the first attempt failed
    let mut delays = config.iterator();
    while let Some(delay) = delays.next() {
        reporter.retry(&location, &last_err, delay);

        tokio::time::sleep(delay).await;

        last_err = match client.get_size(location.clone()).await {
            Ok(v) => return Ok(v),
            Err(err) if err.is_retryable() => err,
            Err(err) => return Err(err),
        };
    }

    return Err(last_err);
}

/// Retries on attempts to get a stream.
/// 
/// If a stream breaks with an [IO] error retries to get
/// a new stream starting where the broken one ended will be made.
async fn retry_download<C, R>(
    client: &C,
    location: C::Location,
    spec: DownloadSpec,
    config: &RetryConfig,
    reporter: &R,
) -> Result<(BytesStream, BytesHint), CondowError>
where
    C: CondowClient,
    R: Reporter,
{
    // The initial stream for the whole download
    let (stream, bytes_hint) =
        retry_download_get_stream(client, location.clone(), spec, config, reporter).await?;

    // Only if we have an upper bound we can try to continue broken streams
    // because we can only download whole BLOBs or ranges. We use a range for
    // the complete BLOB to be able to determine the remainder after a stream broke.
    let original_range = if let Some(expected_bytes) = bytes_hint.upper_bound() {
        InclusiveRange(spec.start(), spec.start() + expected_bytes)
    } else {
        return Ok((stream, bytes_hint));
    };

    // The returned stream is a channel so that we can continue easily after a stream broke
    let (next_elem_tx, output_stream_rx) = mpsc::unbounded();

    // Now we try to complete the stream by requesting new streams wit the remaining
    // bytes if a stream broke
    let _ = tokio::spawn(loop_retry_complete_stream(
        stream,
        location.clone(),
        original_range,
        client.clone(),
        next_elem_tx,
        config.clone(),
        reporter.clone(),
    ));

    Ok((Box::pin(output_stream_rx), bytes_hint))
}

/// Tries to complete the given stream.
/// 
/// If a stream breaks it tries to complete the `original_range` by
/// requesting new stream for the remainder of `original_range` 
async fn loop_retry_complete_stream<C, R>(
    mut stream: BytesStream,
    location: C::Location,
    original_range: InclusiveRange,
    client: C,
    next_elem_tx: mpsc::UnboundedSender<Result<Bytes, IoError>>,
    config: RetryConfig,
    reporter: R,
) where
    C: CondowClient,
    R: Reporter,
{
    // We move the start of the range forward to determine the range
    // we need to complete the original download defined by `original_range`
    let mut remaining_range = original_range;
    loop {
        if let Err((stream_io_error, bytes_read)) = consume_stream(stream, &next_elem_tx).await {
            reporter.stream_broke(&location, &stream_io_error, original_range, remaining_range);
            // we start right after where the previous one ended
            remaining_range.0 += bytes_read + 1;
            let new_spec = DownloadSpec::Range(remaining_range);
            match retry_download_get_stream(&client, location.clone(), new_spec, &config, &reporter)
                .await
            {
                Ok((new_stream, _)) => {
                    stream = new_stream;
                }
                Err(err_new_stream) => {
                    // we must send the final error over the stream
                    let _ = next_elem_tx.unbounded_send(Err(IoError(format!(
                        "failed to create a new stream with error \"{}\"\
                         after previous stream broke with \"{}\"",
                        err_new_stream, stream_io_error
                    ))));
                    break;
                }
            }
        } else {
            // consumption of the stream completed successfully
            break;
        }
    }
}

/// Consume a stream until it is finished or broken.
/// 
/// If it finished [Ok] will be returned otherwise an [Err] containing
/// the bytes read and the [IoError].
async fn consume_stream<St: Stream<Item = Result<Bytes, IoError>>>(
    stream: St,
    next_elem_tx: &mpsc::UnboundedSender<Result<Bytes, IoError>>,
) -> Result<(), (IoError, u64)> {
    let mut stream = Box::pin(stream);

    let mut bytes_read = 0;
    while let Some(chunk_res) = stream.next().await {
        match chunk_res {
            Ok(bytes) => {
                bytes_read += bytes.len() as u64;
                if next_elem_tx.unbounded_send(Ok(bytes)).is_err() {
                    return Ok(());
                }
            }
            Err(err) => {
                // Do not send the error to the stream. We will try to continue
                return Err((err, bytes_read));
            }
        }
    }

    Ok(())
}

/// Retries to get a new stream for the given download spec.
async fn retry_download_get_stream<C, R>(
    client: &C,
    location: C::Location,
    spec: DownloadSpec,
    config: &RetryConfig,
    reporter: &R,
) -> Result<(BytesStream, BytesHint), CondowError>
where
    C: CondowClient,
    R: Reporter,
{
    // The first attempt
    let mut last_err = match client.download(location.clone(), spec).await {
        Ok(stream_and_hint) => return Ok(stream_and_hint),
        Err(err) if err.is_retryable() => err,
        Err(err) => return Err(err),
    };

    // Retries if the first attempt failed
    let mut delays = config.iterator();
    while let Some(delay) = delays.next() {
        reporter.retry(&location, &last_err, delay);

        tokio::time::sleep(delay).await;

        last_err = match client.download(location.clone(), spec).await {
            Ok(stream_and_hint) => return Ok(stream_and_hint),
            Err(err) if err.is_retryable() => err,
            Err(err) => return Err(err),
        };
    }

    return Err(last_err);
}
