use std::{sync::Arc, time::Duration};

use anyhow::{bail, Error as AnyError};
use futures::Future;
use tracing::warn;

use crate::{
    condow_client::CondowClient,
    errors::CondowError,
    probe::Probe,
    streams::{BytesHint, BytesStream},
    InclusiveRange,
};

use retry_stream::RetryPartStream;

pub(crate) mod retry_stream;

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
        Self(dur.as_millis() as u64)
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
    #[doc="This value should be greater than zero to be able to retry on failures where parts of a strem have already been published."]
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
    ///
    /// If disabled the whole download will fail if a single stream of
    /// a part breaks since chunks might already have benn published which must not
    /// be published again.
    pub max_stream_resume_attempts: RetryMaxStreamResumeAttempts,
    // TODO: Add possibility to jitter
}

impl RetryConfig {
    env_ctors!(no_fill);

    /// Set the maximum number of attempts for retries
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
    ///
    /// If disabled the whole download will fail if a single stream of
    /// a part breaks since chunks might already have benn published which must not
    /// be published again.
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
pub struct ClientRetryWrapper<C> {
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

    pub async fn get_size<P: Probe + Clone>(
        &self,
        location: C::Location,
        probe: &P,
    ) -> Result<u64, CondowError> {
        //debug!("getting size");

        let (client, config) = self.inner.as_ref();
        let f = async {
            if let Some(config) = config {
                retry_get_size(client, location, config, probe).await
            } else {
                Ok(client.get_size(location).await?)
            }
        };

        f.await
    }

    pub fn download<P: Probe + Clone>(
        &self,
        location: C::Location,
        range: InclusiveRange,
        probe: P,
    ) -> impl Future<Output = Result<BytesStream, CondowError>> + Send + 'static {
        //debug!("retry client - downloading part");

        let inner = Arc::clone(&self.inner);
        async move {
            let (client, config) = inner.as_ref();
            if let Some(config) = config {
                let stream =
                    RetryPartStream::from_client(client, location, range, config.clone(), probe)
                        .await?;
                Ok(BytesStream::new(stream, BytesHint::new_exact(range.len())))
            } else {
                Ok(client.download(location, range).await?)
            }
        }
    }

    /// Returns the inner [CondowClient]
    #[allow(dead_code)]
    pub fn inner_client(&self) -> &C {
        &self.inner.0
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
async fn retry_get_size<C, P: Probe + Clone>(
    client: &C,
    location: C::Location,
    config: &RetryConfig,
    probe: &P,
) -> Result<u64, CondowError>
where
    C: CondowClient,
{
    // The first attempt
    let mut last_err = match client.get_size(location.clone()).await {
        Ok(v) => return Ok(v),
        Err(err) if err.is_retryable() => err,
        Err(err) => return Err(err),
    };

    // Retries if the first attempt failed
    let mut delays = config.iterator();
    for delay in delays.by_ref() {
        warn!("get size request failed with \"{last_err}\" - retry in {delay:?}");
        probe.retry_attempt(&last_err, delay);

        tokio::time::sleep(delay).await;

        last_err = match client.get_size(location.clone()).await {
            Ok(v) => return Ok(v),
            Err(err) if err.is_retryable() => err,
            Err(err) => return Err(err),
        };
    }

    Err(last_err)
}
