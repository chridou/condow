use std::{sync::Arc, time::Duration};

use anyhow::{bail, Error as AnyError};
use bytes::Bytes;
use futures::{channel::mpsc, Stream, StreamExt};

use crate::{
    condow_client::{CondowClient, DownloadSpec},
    errors::{CondowError, IoError},
    reporter::Reporter,
    streams::{BytesHint, BytesStream},
    InclusiveRange,
};

#[cfg(test)]
mod tests;

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

impl From<Duration> for RetryInitialDelayMs {
    fn from(dur: Duration) -> Self {
        Self(dur.as_micros() as u64)
    }
}

new_type! {
    #[doc="The factor the previous retry is multiplied by."]
    #[doc="This is actually what makes it exponentially when greater than 1.0."]
    #[doc="Default is 1.5."]
    #[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
    pub copy struct RetryDelayFactor(f64, env="RETRY_DELAY_FACTOR");
}

impl Default for RetryDelayFactor {
    fn default() -> Self {
        Self(1.5)
    }
}

impl From<i32> for RetryDelayFactor {
    fn from(f: i32) -> Self {
        Self(f as f64)
    }
}

new_type! {
    #[doc="The maximum retry for a retry attempt in milliseconds."]
    #[doc="Default is 5 seconds."]
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

impl From<Duration> for RetryDelayMaxMs {
    fn from(dur: Duration) -> Self {
        Self(dur.as_micros() as u64)
    }
}

new_type! {
    #[doc="The maximum number of attempts to resume a byte stream from the same offset."]
    #[doc="Default is 3."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub copy struct RetryMaxStreamResumeAttempts(usize, env="RETRY_MAX_STREAM_RESUME_ATTEMPTS");
}

impl Default for RetryMaxStreamResumeAttempts {
    fn default() -> Self {
        Self(3)
    }
}

/// Configures retries with exponential backoff
///
/// # Overview
///
/// Retries can be done on the downloads themselves
/// as well on the byte streams returned from a client. If an error occurs
/// while streaming bytes ConDow will try to reconnect with retries and
/// continue streaming where the previous stream failed.
/// Streaming will always be aborted if no bytes were read 3 times in a row even
/// though successful requests for a stream were made.
///
/// Retries can also be attempted on size requests.
///
/// Be aware that some clients might also do retries themselves based on
/// their underlying implementation. In this case you should disable retries for either the
/// client or ConDow itself.
///
/// # Limitations
///
/// ConDow can only try to resume on byte stream errors if the underlying [CondowClient]
/// returns an exact bound on the number of bytes returned when a new stream is created.
#[derive(Debug, Default, Clone, PartialEq)]
#[non_exhaustive]
pub struct RetryConfig {
    /// The maximum number of retry attempts.
    pub max_attempts: RetryMaxAttempts,
    /// The delay before the first retry attempt.
    pub initial_delay_ms: RetryInitialDelayMs,
    /// A factor to multiply a current delay with to get the next one
    pub delay_factor: RetryDelayFactor,
    /// The maximum delay for a retry.
    pub max_delay_ms: RetryDelayMaxMs,
    /// The maximum number of attempts to resume a byte stream from the same offset.
    ///
    /// Setting this to 0 will disable resumes. Enabling them has a small overhead
    /// since the current progress on a byte stream must be tracked.
    pub max_stream_resume_attempts: RetryMaxStreamResumeAttempts,
    // TODO: Add possibility to jitter
}

impl RetryConfig {
    env_ctors!(no_fill);

    /// Set the maximum number of attempt for retries
    pub fn max_attempts<T: Into<RetryMaxAttempts>>(mut self, max_attempts: T) -> Self {
        self.max_attempts = max_attempts.into();
        self
    }

    /// Set the delay for the first retry attempt after the original operation failed
    pub fn initial_delay_ms<T: Into<RetryInitialDelayMs>>(mut self, initial_delay_ms: T) -> Self {
        self.initial_delay_ms = initial_delay_ms.into();
        self
    }

    /// Set the factor by which each current delay will be multiplied by to get the next delay
    ///
    /// This is actually what makes it exponentially when greater than 1.0.
    pub fn delay_factor<T: Into<RetryDelayFactor>>(mut self, delay_factor: T) -> Self {
        self.delay_factor = delay_factor.into();
        self
    }

    /// Set the maximum duration in milliseconds for a single delay
    pub fn max_delay_ms<T: Into<RetryDelayMaxMs>>(mut self, max_delay_ms: T) -> Self {
        self.max_delay_ms = max_delay_ms.into();
        self
    }

    /// The maximum number of attempts to resume a byte stream from the same offset.
    ///
    /// Setting this to 0 will disable resumes. Enabling them has a small overhead
    /// since the current progress on a byte stream must be tracked.    
    pub fn max_stream_resume_attempts<T: Into<RetryMaxStreamResumeAttempts>>(
        mut self,
        max_stream_resume_attempts: T,
    ) -> Self {
        self.max_stream_resume_attempts = max_stream_resume_attempts.into();
        self
    }

    /// Disable attempts to resume a byte stream from the same offset.
    pub fn no_stream_resume_attempts(mut self) -> Self {
        self.max_stream_resume_attempts = 0.into();
        self
    }

    /// Validate this [RetryConfig]
    ///
    /// Succeeds if
    /// * `delay_factor` is at least 1.0
    /// * `delay_factor` is a number
    pub fn validate(&self) -> Result<(), AnyError> {
        if self.delay_factor.0 < 1.0 {
            bail!("'delay_factor' must be at least 1.0");
        }

        if self.delay_factor.0.is_nan() {
            bail!("'delay_factor' must not be NaN");
        }

        if self.delay_factor.0.is_infinite() {
            bail!("'delay_factor' must not be infinite");
        }

        Ok(())
    }

    /// Validate this [RetryConfig] and return it if it is valid.
    ///
    /// Can be used as a finalizer after builder style construction.
    ///
    /// See also [RetryConfig::validate]
    pub fn validated(self) -> Result<Self, AnyError> {
        self.validate()?;
        Ok(self)
    }

    /// Create an [Iterator] of delays to be applied before each retry attempt
    ///
    /// The iterator doesn't make any assumptions on whether the configuration it
    /// was created from is meaningful from a users perspective. Therefore
    /// `RetryConfig::validate` should be called before creating a
    /// [RetryDelaysIterator].
    pub(crate) fn iterator(&self) -> impl Iterator<Item = Duration> {
        RetryDelaysIterator::new(
            self.max_attempts.into_inner(),
            self.initial_delay_ms.into_inner() as f64 / 1_000.0,
            self.max_delay_ms.into_inner() as f64 / 1_000.0,
            self.delay_factor.into_inner(),
        )
    }

    fn fill_from_env_prefixed_internal<T: AsRef<str>>(
        &mut self,
        prefix: T,
    ) -> Result<bool, AnyError> {
        let mut found_any = false;

        if let Some(max_attempts) = RetryMaxAttempts::try_from_env_prefixed(prefix.as_ref())? {
            found_any = true;
            self.max_attempts = max_attempts;
        }
        if let Some(initial_delay_ms) = RetryInitialDelayMs::try_from_env_prefixed(prefix.as_ref())?
        {
            found_any = true;
            self.initial_delay_ms = initial_delay_ms;
        }
        if let Some(delay_factor) = RetryDelayFactor::try_from_env_prefixed(prefix.as_ref())? {
            found_any = true;
            self.delay_factor = delay_factor;
        }
        if let Some(max_delay_ms) = RetryDelayMaxMs::try_from_env_prefixed(prefix.as_ref())? {
            found_any = true;
            self.max_delay_ms = max_delay_ms;
        }

        Ok(found_any)
    }
}

/// An [Iterator] over delays to be applied before each retry
///
/// The iterator returns a number of delays as
/// there are retry attempts configured.
struct RetryDelaysIterator {
    attempts_left: usize,
    next_delay_secs: f64,
    max_delay_secs: f64,
    delay_factor: f64,
}

impl RetryDelaysIterator {
    /// Creates a new instance
    ///
    /// All [f64]s are fused to be at least 0.0.
    fn new(
        attempts_left: usize,
        next_delay_secs: f64,
        max_delay_secs: f64,
        delay_factor: f64,
    ) -> Self {
        Self {
            attempts_left,
            next_delay_secs: next_delay_secs.max(0.0),
            max_delay_secs: max_delay_secs.max(0.0),
            delay_factor: delay_factor.max(0.0),
        }
    }
}

impl Iterator for RetryDelaysIterator {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        if self.attempts_left == 0 {
            return None;
        }

        self.attempts_left -= 1;

        let current_delay_secs = self.next_delay_secs;

        // No check for overflows etc. since the maximum here is propably
        // farther in the future than humanity will exist.
        self.next_delay_secs *= self.delay_factor;

        let delay = Duration::from_secs_f64(current_delay_secs.min(self.max_delay_secs));
        Some(delay)
    }
}

/// Wraps any [CondowClient] to add retry behaviour
///
/// If a [RetryConfig] is present it will be applied
/// to perform retries otherwise the client will
/// be called directly without any additional logic.
///
/// The methods need a [Reporter] which is used to signal
/// retries and broken streams.
#[derive(Clone)]
pub(crate) struct ClientRetryWrapper<C> {
    inner: Arc<(C, Option<RetryConfig>)>,
}

impl<C> ClientRetryWrapper<C>
where
    C: CondowClient,
{
    pub fn new(client: C, config: Option<RetryConfig>) -> Self {
        Self {
            inner: Arc::new((client, config)),
        }
    }

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

#[cfg(test)]
impl<C> From<C> for ClientRetryWrapper<C>
where
    C: CondowClient,
{
    fn from(client: C) -> Self {
        Self::new(client, None)
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
        reporter.retry_attempt(&location, &last_err, delay);

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
/// If a stream breaks with an [IoError] retries to get
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

    // Only if we have an length we can try to continue broken streams
    // because we can only download whole BLOBs or ranges. We use a range for
    // the complete BLOB to be able to determine the remainder after a stream broke.
    // If the mximum number to resume is 0 we also do not want to resume on broken streams.
    let blob_len_for_resume = bytes_hint.exact().and_then(|blob_len| {
        if config.max_stream_resume_attempts.into_inner() > 0 {
            Some(blob_len)
        } else {
            None
        }
    });

    let original_range = if let Some(blob_len_for_resume) = blob_len_for_resume {
        InclusiveRange(spec.start(), spec.start() + blob_len_for_resume - 1) // original range has at least len 1
    } else {
        // We are done because we will not do any resume attempts
        return Ok((stream, bytes_hint));
    };

    // The returned stream is a channel so that we can continue easily after a stream broke
    let (next_elem_tx, output_stream_rx) = mpsc::unbounded();

    // Now we try to complete the stream by requesting new streams with the remaining
    // bytes if a stream broke
    tokio::spawn(loop_retry_complete_stream(
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
    let mut n_times_made_no_progress = 0;
    loop {
        if let Err((stream_io_error, bytes_read)) = try_consume_stream(stream, &next_elem_tx).await
        {
            if bytes_read > 0 {
                // we start right after where the previous one ended
                remaining_range.0 += bytes_read;
                n_times_made_no_progress = 0;
            } else {
                // Hint: Failing on the first byte also counts as
                // not having made any progress.
                n_times_made_no_progress += 1;
            }

            if n_times_made_no_progress >= config.max_stream_resume_attempts.into_inner() {
                let _ = next_elem_tx.unbounded_send(Err(IoError(format!(
                    "failed to make progress on the stream {} times \
                    with the last error being \"{}\"",
                    n_times_made_no_progress, stream_io_error
                ))));
                break;
            }

            let new_spec = DownloadSpec::Range(remaining_range);
            reporter.stream_resume_attempt(
                &location,
                &stream_io_error,
                original_range,
                remaining_range,
            );
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
async fn try_consume_stream<St: Stream<Item = Result<Bytes, IoError>>>(
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
        reporter.retry_attempt(&location, &last_err, delay);

        tokio::time::sleep(delay).await;

        last_err = match client.download(location.clone(), spec).await {
            Ok(stream_and_hint) => return Ok(stream_and_hint),
            Err(err) if err.is_retryable() => err,
            Err(err) => return Err(err),
        };
    }

    return Err(last_err);
}
