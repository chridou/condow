//! Configuration items
//!
//! Many of the fields of [Config] are
//! implemented with the new type pattern.
//! This is mainly to allow them to be
//! initialized from the environment.
use std::{
    fmt, ops,
    str::{from_utf8, FromStr},
    time::Duration,
};

use anyhow::{bail, Error as AnyError};
use tracing::{debug, info};

pub use crate::retry::*;

/// A configuration for [Condow](super::Condow).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Config {
    /// Size in bytes of the parts the download is split into
    ///
    /// Default is 4 Mebi.
    pub part_size_bytes: PartSizeBytes,
    /// The maximum concurrency for a single download.
    ///
    /// Actual concurrency can be lower if less parts
    /// than `max_concurrency` are to be
    /// downloaded
    pub max_concurrency: MaxConcurrency,
    /// The minimum number of parts a download must consist of for the parts to be downloaded concurrently.
    /// Depending on the part sizes it might be more efficient to set a number higer than 2. Downloading concurrently has an overhead.
    /// This setting plays together with `MinBytesForConcurrentDownload`.
    /// Setting this value to 0 or 1 makes no sense and has no effect.
    /// The default is 2.
    pub min_parts_for_concurrent_download: MinPartsForConcurrentDownload,
    /// The minimum number of bytes a download must consist of for the parts to be downloaded concurrently.
    /// Downloading concurrently has an overhead so it can make sens to set this value greater that `PART_SIZE_BYTES`.
    ///
    /// This setting plays together with `MinPartsForConcurrentDownload`.
    ///
    /// The default is 0.
    pub min_bytes_for_concurrent_download: MinBytesForConcurrentDownload,
    /// Define how parts for a sequential download should be treated.
    ///
    /// A sequential download can not only be triggered by just one part being
    /// requested but also by the [MinBytesForConcurrentDownload] and [MinPartsForConcurrentDownload]
    /// configuration values.
    ///
    /// The default is [SequentialDownloadMode::SingleDownload].
    pub sequential_download_mode: SequentialDownloadMode,
    /// Make sure that the network stream is always actively pulled into an intermediate buffer.
    ///
    /// This is not always the case since some low concurrency downloads require the strem to be actively pulled.
    /// This also allows for detection of panics.
    ///
    /// The default is `false` which means this feature is turned off.
    pub ensure_active_pull: EnsureActivePull,
    /// Size of the buffer for each download task.
    ///
    /// If set to 0 (not advised) there will be now buffer at all.
    /// This should be configured so that none of the tasks runs ever empty
    ///
    /// Default is 2
    pub buffer_size: BufferSize,
    /// If all buffers of all download tasks are full, this is the maximum time
    /// to pause until the next attempt.
    ///
    /// Default is 10ms
    pub max_buffers_full_delay_ms: MaxBuffersFullDelayMs,
    /// If set to `true` download related messages are logged at `DEBUG` level. Otherwise at `INFO` level.
    ///
    /// The affectd messages are:
    /// * `Download started`
    /// * `Download completed`
    /// * `Download failed`
    pub log_download_messages_as_debug: LogDownloadMessagesAsDebug,
    /// Configures retries if there.
    ///
    /// Otherwise there won't be any retry attempts made
    ///
    /// Retries are turned on by default
    pub retries: Option<RetryConfig>,
}

impl Config {
    env_ctors!(no_fill);

    /// Set the size of the parts the download is split into in bytes
    pub fn part_size_bytes<T: Into<PartSizeBytes>>(mut self, part_size_bytes: T) -> Self {
        self.part_size_bytes = part_size_bytes.into();
        self
    }

    /// Set the maximum concurrency of a download
    pub fn max_concurrency<T: Into<MaxConcurrency>>(mut self, max_concurrency: T) -> Self {
        self.max_concurrency = max_concurrency.into();
        self
    }

    /// The minimum number of parts a download must consist of for the parts to be downloaded concurrently.
    /// Depending on the part sizes it might be more efficient to set a number higer than 2. Downloading concurrently has an overhead.
    /// This setting plays together with `MinBytesForConcurrentDownload`.
    /// Setting this value to 0 or 1 makes no sense and has no effect.
    /// The default is 2.
    pub fn min_parts_for_concurrent_download<T: Into<MinPartsForConcurrentDownload>>(
        mut self,
        min_parts_for_concurrent_download: T,
    ) -> Self {
        self.min_parts_for_concurrent_download = min_parts_for_concurrent_download.into();
        self
    }

    /// The minimum number of bytes a download must consist of for the parts to be downloaded concurrently.
    /// Downloading concurrently has an overhead so it can make sens to set this value greater that `PART_SIZE_BYTES`.
    ///
    /// This setting plays together with `MinPartsForConcurrentDownload`.
    ///
    /// The default is 0.
    pub fn min_bytes_for_concurrent_download<T: Into<MinBytesForConcurrentDownload>>(
        mut self,
        min_bytes_for_concurrent_download: T,
    ) -> Self {
        self.min_bytes_for_concurrent_download = min_bytes_for_concurrent_download.into();
        self
    }

    /// The minimum number of bytes a download must consist of for the parts to be downloaded concurrently.
    /// Downloading concurrently has an overhead so it can make sens to set this value greater that `PART_SIZE_BYTES`.
    ///
    /// This setting plays together with `MinPartsForConcurrentDownload`.
    ///
    /// The default is 0.
    pub fn sequential_download_mode<T: Into<SequentialDownloadMode>>(
        mut self,
        sequential_download_mode: T,
    ) -> Self {
        self.sequential_download_mode = sequential_download_mode.into();
        self
    }

    /// Make sure that the network stream is always actively pulled into an intermediate buffer.
    ///
    /// This is not always the case since some low concurrency downloads require the strem to be actively pulled.
    /// This also allows for detection of panics.
    ///
    /// The default is `false` which means this feature is turned off.
    pub fn ensure_active_pull<T: Into<EnsureActivePull>>(mut self, ensure_active_pull: T) -> Self {
        self.ensure_active_pull = ensure_active_pull.into();
        self
    }

    /// Set the size of the buffer for each download task.
    pub fn buffer_size<T: Into<BufferSize>>(mut self, buffer_size: T) -> Self {
        self.buffer_size = buffer_size.into();
        self
    }

    /// Set the maximum delay in case all task buffers are full before a retry
    /// to enqueue the next downlod part is made.
    pub fn max_buffers_full_delay_ms<T: Into<MaxBuffersFullDelayMs>>(
        mut self,
        max_buffers_full_delay_ms: T,
    ) -> Self {
        self.max_buffers_full_delay_ms = max_buffers_full_delay_ms.into();
        self
    }

    /// If set to `true` download related messages are logged at `DEBUG` level. Otherwise at `INFO` level.
    ///
    /// The affectd messages are:
    /// * `download started`
    /// * `download completed`
    /// * `download failed`
    pub fn log_download_messages_as_debug<T: Into<LogDownloadMessagesAsDebug>>(
        mut self,
        log_download_messages_as_debug: T,
    ) -> Self {
        self.log_download_messages_as_debug = log_download_messages_as_debug.into();
        self
    }

    /// Enables retries with the given configuration
    pub fn retries(mut self, config: RetryConfig) -> Self {
        self.retries = Some(config);
        self
    }

    /// Configure retries
    ///
    /// Uses the currently configured [RetryConfig] or the default of [RetryConfig]
    /// if none is configured
    pub fn configure_retries<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(RetryConfig) -> RetryConfig,
    {
        let retries = self.retries.take().unwrap_or_default();
        self.retries(f(retries))
    }

    /// Configure retries starting with the default
    pub fn configure_retries_from_default<F>(self, mut f: F) -> Self
    where
        F: FnMut(RetryConfig) -> RetryConfig,
    {
        self.retries(f(RetryConfig::default()))
    }

    /// Disables retries
    ///
    /// Retries are enabled by default.
    pub fn disable_retries(mut self) -> Self {
        self.retries = None;
        self
    }

    /// Validate this [Config]
    pub fn validated(self) -> Result<Self, AnyError> {
        self.validate()?;
        Ok(self)
    }

    /// Validate this [Config]
    pub fn validate(&self) -> Result<(), AnyError> {
        if self.max_concurrency.0 == 0 {
            bail!("'max_concurrency' must not be 0");
        }

        if self.part_size_bytes.0 == 0 {
            bail!("'part_size_bytes' must not be 0");
        }

        if let Some(retries) = &self.retries {
            retries.validate()?;
        }

        Ok(())
    }

    fn fill_from_env_prefixed_internal<T: AsRef<str>>(
        &mut self,
        prefix: T,
    ) -> Result<bool, AnyError> {
        let mut found_any = false;

        if let Some(part_size_bytes) = PartSizeBytes::try_from_env_prefixed(prefix.as_ref())? {
            found_any = true;
            self.part_size_bytes = part_size_bytes;
        }
        if let Some(max_concurrency) = MaxConcurrency::try_from_env_prefixed(prefix.as_ref())? {
            found_any = true;
            self.max_concurrency = max_concurrency;
        }
        if let Some(min_parts_for_concurrent_download) =
            MinPartsForConcurrentDownload::try_from_env_prefixed(prefix.as_ref())?
        {
            found_any = true;
            self.min_parts_for_concurrent_download = min_parts_for_concurrent_download;
        }
        if let Some(min_bytes_for_concurrent_download) =
            MinBytesForConcurrentDownload::try_from_env_prefixed(prefix.as_ref())?
        {
            found_any = true;
            self.min_bytes_for_concurrent_download = min_bytes_for_concurrent_download;
        }
        if let Some(sequential_download_mode) =
            SequentialDownloadMode::try_from_env_prefixed(prefix.as_ref())?
        {
            found_any = true;
            self.sequential_download_mode = sequential_download_mode;
        }
        if let Some(ensure_active_pull) = EnsureActivePull::try_from_env_prefixed(prefix.as_ref())?
        {
            found_any = true;
            self.ensure_active_pull = ensure_active_pull;
        }
        if let Some(buffer_size) = BufferSize::try_from_env_prefixed(prefix.as_ref())? {
            found_any = true;
            self.buffer_size = buffer_size;
        }
        if let Some(max_buffers_full_delay_ms) =
            MaxBuffersFullDelayMs::try_from_env_prefixed(prefix.as_ref())?
        {
            found_any = true;
            self.max_buffers_full_delay_ms = max_buffers_full_delay_ms;
        }

        if let Some(log_download_messages_as_debug) =
            LogDownloadMessagesAsDebug::try_from_env_prefixed(prefix.as_ref())?
        {
            found_any = true;
            self.log_download_messages_as_debug = log_download_messages_as_debug;
        }

        if let Some(retries) = RetryConfig::from_env_prefixed(prefix.as_ref())? {
            found_any = true;
            self.retries = Some(retries);
        }

        Ok(found_any)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            part_size_bytes: Default::default(),
            max_concurrency: Default::default(),
            min_bytes_for_concurrent_download: Default::default(),
            min_parts_for_concurrent_download: Default::default(),
            sequential_download_mode: Default::default(),
            ensure_active_pull: Default::default(),
            buffer_size: Default::default(),
            max_buffers_full_delay_ms: Default::default(),
            log_download_messages_as_debug: Default::default(),
            retries: Some(Default::default()),
        }
    }
}

/// Size of the parts in bytes a download is split into
///
/// # Examples
///
/// ### Parsing
/// ```rust
/// # use condow_core::config::PartSizeBytes;
///
/// let n_bytes: PartSizeBytes = "34".parse().unwrap();
/// assert_eq!(n_bytes, 34.into());
///
/// let n_bytes: PartSizeBytes = "1k".parse().unwrap();
/// assert_eq!(n_bytes, 1_000.into());
///
/// let n_bytes: PartSizeBytes = "1 k".parse().unwrap();
/// assert_eq!(n_bytes, 1_000.into());
///
/// let n_bytes: PartSizeBytes = "1M".parse().unwrap();
/// assert_eq!(n_bytes, 1_000_000.into());
///
/// let n_bytes: PartSizeBytes = "1G".parse().unwrap();
/// assert_eq!(n_bytes, 1_000_000_000.into());
///
/// let n_bytes: PartSizeBytes = "1Ki".parse().unwrap();
/// assert_eq!(n_bytes, 1_024.into());
///
/// let n_bytes: PartSizeBytes = "1Mi".parse().unwrap();
/// assert_eq!(n_bytes, 1_048_576.into());
///
/// let n_bytes: PartSizeBytes = "1Gi".parse().unwrap();
/// assert_eq!(n_bytes, 1_073_741_824.into());
///
/// // Case sensitive
/// let res = "1K".parse::<PartSizeBytes>();
/// assert!(res.is_err());
///
/// let res = "x".parse::<PartSizeBytes>();
/// assert!(res.is_err());
///
/// let res = "".parse::<PartSizeBytes>();
/// assert!(res.is_err());
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PartSizeBytes(u64);

impl PartSizeBytes {
    pub fn new<T: Into<u64>>(part_size_bytes: T) -> Self {
        Self(part_size_bytes.into())
    }

    pub const fn from_u64(part_size_bytes: u64) -> Self {
        Self(part_size_bytes)
    }

    pub const fn into_inner(self) -> u64 {
        self.0
    }

    env_funs!("PART_SIZE_BYTES");
}

impl fmt::Display for PartSizeBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ops::Deref for PartSizeBytes {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for PartSizeBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for PartSizeBytes {
    fn default() -> Self {
        PartSizeBytes::new(Mebi(4))
    }
}

impl From<u64> for PartSizeBytes {
    fn from(v: u64) -> Self {
        PartSizeBytes(v)
    }
}

impl From<PartSizeBytes> for u64 {
    fn from(v: PartSizeBytes) -> Self {
        v.0
    }
}

impl From<Kilo> for PartSizeBytes {
    fn from(v: Kilo) -> Self {
        PartSizeBytes::new(v)
    }
}

impl From<Mega> for PartSizeBytes {
    fn from(v: Mega) -> Self {
        PartSizeBytes::new(v)
    }
}

impl From<Giga> for PartSizeBytes {
    fn from(v: Giga) -> Self {
        PartSizeBytes::new(v)
    }
}

impl From<Kibi> for PartSizeBytes {
    fn from(v: Kibi) -> Self {
        PartSizeBytes::new(v)
    }
}

impl From<Mebi> for PartSizeBytes {
    fn from(v: Mebi) -> Self {
        PartSizeBytes::new(v)
    }
}

impl From<Gibi> for PartSizeBytes {
    fn from(v: Gibi) -> Self {
        PartSizeBytes::new(v)
    }
}

impl FromStr for PartSizeBytes {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<UnitPrefix>().map(|up| PartSizeBytes(up.value()))
    }
}
new_type! {
    #[doc="Maximum concurrency of a single download"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct MaxConcurrency(usize, env="MAX_CONCURRENCY");
}

impl MaxConcurrency {
    pub const fn from_usize(max: usize) -> Self {
        Self(max)
    }
}

impl Default for MaxConcurrency {
    fn default() -> Self {
        MaxConcurrency(64)
    }
}

new_type! {
    #[doc="Buffer size of a concurrent download task"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct BufferSize(usize, env="BUFFER_SIZE");
}

impl Default for BufferSize {
    fn default() -> Self {
        BufferSize(2)
    }
}

new_type! {
    #[doc="Make sure that the network stream is always actively pulled into an intermediate buffer."]
    #[doc="This is not always the case since some low concurrency downloads require the strem to be actively pulled."]
    #[doc="This also allows for detection of panics."]
    #[doc="The default is `false` which means this feature is turned off."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct EnsureActivePull(bool, env="ENSURE_ACTIVE_PULL");
}

impl Default for EnsureActivePull {
    fn default() -> Self {
        EnsureActivePull(false)
    }
}

new_type! {
    #[doc="The minimum number of parts a download must consist of for the parts to be downloaded concurrently"]
    #[doc="Depending on the part sizes it might be more efficient to set a number higer than 2. Downloading concurrently has an overhead."]
    #[doc="This setting plays together with `MinBytesForConcurrentDownload`."]
    #[doc="Setting this value to 0 or 1 makes no sense and has no effect."]
    #[doc="The default is 2."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct MinPartsForConcurrentDownload(u64, env="MIN_PARTS_FOR_CONCURRENT_DOWNLOAD");
}

impl Default for MinPartsForConcurrentDownload {
    fn default() -> Self {
        MinPartsForConcurrentDownload(2)
    }
}

/// The minimum number of bytes a download must consist of for the parts to be downloaded concurrently.
/// Downloading concurrently has an overhead so it can make sens to set this value greater that `PART_SIZE_BYTES`.
///
/// This setting plays together with `MinPartsForConcurrentDownload`.
///
/// The default is 0.
///
/// # Examples
///
/// ### Parsing
/// ```rust
/// # use condow_core::config::MinBytesForConcurrentDownload;
///
/// let n_bytes: MinBytesForConcurrentDownload = "34".parse().unwrap();
/// assert_eq!(n_bytes, 34.into());
///
/// let n_bytes: MinBytesForConcurrentDownload = "1k".parse().unwrap();
/// assert_eq!(n_bytes, 1_000.into());
///
/// let n_bytes: MinBytesForConcurrentDownload = "1 k".parse().unwrap();
/// assert_eq!(n_bytes, 1_000.into());
///
/// let n_bytes: MinBytesForConcurrentDownload = "1M".parse().unwrap();
/// assert_eq!(n_bytes, 1_000_000.into());
///
/// let n_bytes: MinBytesForConcurrentDownload = "1G".parse().unwrap();
/// assert_eq!(n_bytes, 1_000_000_000.into());
///
/// let n_bytes: MinBytesForConcurrentDownload = "1Ki".parse().unwrap();
/// assert_eq!(n_bytes, 1_024.into());
///
/// let n_bytes: MinBytesForConcurrentDownload = "1Mi".parse().unwrap();
/// assert_eq!(n_bytes, 1_048_576.into());
///
/// let n_bytes: MinBytesForConcurrentDownload = "1Gi".parse().unwrap();
/// assert_eq!(n_bytes, 1_073_741_824.into());
///
/// // Case sensitive
/// let res = "1K".parse::<MinBytesForConcurrentDownload>();
/// assert!(res.is_err());
///
/// let res = "x".parse::<MinBytesForConcurrentDownload>();
/// assert!(res.is_err());
///
/// let res = "".parse::<MinBytesForConcurrentDownload>();
/// assert!(res.is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MinBytesForConcurrentDownload(u64);

impl MinBytesForConcurrentDownload {
    pub fn new<T: Into<u64>>(part_size_bytes: T) -> Self {
        Self(part_size_bytes.into())
    }

    pub const fn from_u64(part_size_bytes: u64) -> Self {
        Self(part_size_bytes)
    }

    pub const fn into_inner(self) -> u64 {
        self.0
    }

    env_funs!("MIN_BYTES_FOR_CONCURRENT_DOWNLOAD");
}

impl ops::Deref for MinBytesForConcurrentDownload {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for MinBytesForConcurrentDownload {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for MinBytesForConcurrentDownload {
    fn default() -> Self {
        MinBytesForConcurrentDownload::new(0u64)
    }
}

impl From<u64> for MinBytesForConcurrentDownload {
    fn from(v: u64) -> Self {
        MinBytesForConcurrentDownload(v)
    }
}

impl From<MinBytesForConcurrentDownload> for u64 {
    fn from(v: MinBytesForConcurrentDownload) -> Self {
        v.0
    }
}

impl From<Kilo> for MinBytesForConcurrentDownload {
    fn from(v: Kilo) -> Self {
        MinBytesForConcurrentDownload::new(v)
    }
}

impl From<Mega> for MinBytesForConcurrentDownload {
    fn from(v: Mega) -> Self {
        MinBytesForConcurrentDownload::new(v)
    }
}

impl From<Giga> for MinBytesForConcurrentDownload {
    fn from(v: Giga) -> Self {
        MinBytesForConcurrentDownload::new(v)
    }
}

impl From<Kibi> for MinBytesForConcurrentDownload {
    fn from(v: Kibi) -> Self {
        MinBytesForConcurrentDownload::new(v)
    }
}

impl From<Mebi> for MinBytesForConcurrentDownload {
    fn from(v: Mebi) -> Self {
        MinBytesForConcurrentDownload::new(v)
    }
}

impl From<Gibi> for MinBytesForConcurrentDownload {
    fn from(v: Gibi) -> Self {
        MinBytesForConcurrentDownload::new(v)
    }
}

impl FromStr for MinBytesForConcurrentDownload {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<UnitPrefix>()
            .map(|up| MinBytesForConcurrentDownload(up.value()))
    }
}

new_type! {
    #[doc="Maximum time to wait for download buffers when all were full in ms"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct MaxBuffersFullDelayMs(u64, env="MAX_BUFFERS_FULL_DELAY_MS");
}

impl Default for MaxBuffersFullDelayMs {
    fn default() -> Self {
        Self(10)
    }
}

impl From<MaxBuffersFullDelayMs> for Duration {
    fn from(m: MaxBuffersFullDelayMs) -> Self {
        Duration::from_millis(m.0)
    }
}

new_type! {
    #[doc="If set to `true` download related messages are logged at `DEBUG` level. Otherwise at `INFO` level."]
    #[doc="The default is `true` (log on `DEBUG` level)."]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct LogDownloadMessagesAsDebug(bool, env="LOG_DOWNLOAD_MESSAGES_AS_DEBUG");
}

impl LogDownloadMessagesAsDebug {
    pub(crate) fn log<T: AsRef<str>>(self, msg: T) {
        if *self {
            debug!("{}", msg.as_ref());
        } else {
            info!("{}", msg.as_ref());
        }
    }

    /// Returns the inner value
    pub fn value(self) -> bool {
        self.0
    }
}

impl Default for LogDownloadMessagesAsDebug {
    fn default() -> Self {
        LogDownloadMessagesAsDebug(true)
    }
}

/// Define how parts for a sequential download should be treated.
///
/// A sequential download can not only be triggered by just one part being
/// requested but also by the [MinBytesForConcurrentDownload] and [MinPartsForConcurrentDownload]
/// configuration values.
///
/// The default is [SequentialDownloadMode::SingleDownload].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequentialDownloadMode {
    /// Keep it as it is
    KeepParts,
    /// Merge all potential parts into a single one.
    ///
    /// For sequential downloads this is probably the most efficient
    /// since for remote request no new connections
    /// have to be made.
    ///
    /// This is the default
    MergeParts,
    /// Repartition the download into parts with
    /// mostly the given size
    Repartition { part_size: PartSizeBytes },
}

impl SequentialDownloadMode {
    pub fn new<T: Into<SequentialDownloadMode>>(mode: T) -> Self {
        mode.into()
    }

    env_funs!("SEQUENTIAL_DOWNLOAD_MODE");
}

impl Default for SequentialDownloadMode {
    fn default() -> Self {
        Self::MergeParts
    }
}

impl From<PartSizeBytes> for SequentialDownloadMode {
    fn from(part_size: PartSizeBytes) -> Self {
        Self::Repartition { part_size }
    }
}

impl FromStr for SequentialDownloadMode {
    type Err = AnyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        match s {
            "KEEP_PARTS" => Ok(Self::KeepParts),
            "SINGLE_DOWNLOAD" => Ok(Self::MergeParts),
            probably_a_number => {
                let part_size = probably_a_number.parse()?;
                Ok(Self::Repartition { part_size })
            }
        }
    }
}

/// Multiplies by 1 when converted to a u64
///
// # Examples
///
/// ```rust
/// # use condow_core::config::Unit;
/// assert_eq!(u64::from(Unit(2)), 2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Unit(pub u64);

impl Unit {
    /// Returns the value
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl From<Unit> for u64 {
    fn from(m: Unit) -> Self {
        m.value()
    }
}
/// Multiplies by 1_000 when converted to a u64
///
// # Examples
///
/// ```rust
/// # use condow_core::config::Kilo;
/// assert_eq!(u64::from(Kilo(2)), 2*1_000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Kilo(pub u64);

impl Kilo {
    /// Returns the value
    pub const fn value(self) -> u64 {
        self.0 * 1_000
    }
}

impl From<Kilo> for u64 {
    fn from(m: Kilo) -> Self {
        m.value()
    }
}

/// Multiplies by 1_000_000 when converted to a u64
///
// # Examples
///
/// ```rust
/// # use condow_core::config::Mega;
/// assert_eq!(u64::from(Mega(2)), 2*1_000_000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mega(pub u64);

impl Mega {
    /// Returns the value
    pub const fn value(self) -> u64 {
        self.0 * 1_000_000
    }
}

impl From<Mega> for u64 {
    fn from(m: Mega) -> Self {
        m.value()
    }
}

/// Multiplies by 1_000_000_000 when converted to a u64
///
// # Examples
///
/// ```rust
/// # use condow_core::config::Giga;
/// assert_eq!(u64::from(Giga(2)), 2*1_000_000_000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Giga(pub u64);

impl Giga {
    /// Returns the value
    pub const fn value(self) -> u64 {
        self.0 * 1_000_000_000
    }
}

impl From<Giga> for u64 {
    fn from(m: Giga) -> Self {
        m.value()
    }
}

/// Multiplies by 1_024 when converted to a u64
///
// # Examples
///
/// ```rust
/// # use condow_core::config::Kibi;
/// assert_eq!(u64::from(Kibi(2)), 2*1_024);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Kibi(pub u64);

impl Kibi {
    /// Returns the value
    pub const fn value(self) -> u64 {
        self.0 * 1_024
    }
}

impl From<Kibi> for u64 {
    fn from(m: Kibi) -> Self {
        m.value()
    }
}

/// Multiplies by 1_048_576 when converted to a u64
///
// # Examples
///
/// ```rust
/// # use condow_core::config::Mebi;
/// assert_eq!(u64::from(Mebi(2)), 2*1_048_576);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mebi(pub u64);

impl Mebi {
    /// Returns the value
    pub const fn value(self) -> u64 {
        self.0 * 1_048_576
    }
}

impl From<Mebi> for u64 {
    fn from(m: Mebi) -> Self {
        m.value()
    }
}

/// Multiplies by 1_073_741_824 when converted to a u64
//
// # Examples
///
/// ```rust
/// # use condow_core::config::Gibi;
/// assert_eq!(u64::from(Gibi(2)), 2*1_073_741_824);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gibi(pub u64);

impl Gibi {
    /// Returns the value
    pub const fn value(self) -> u64 {
        self.0 * 1_073_741_824
    }
}

impl From<Gibi> for u64 {
    fn from(m: Gibi) -> Self {
        m.value()
    }
}

/// A value with a unit prefix.
///
/// # Examples
///
/// ### Parsing
/// ```rust
/// # use condow_core::config::UnitPrefix;
///
/// let value: UnitPrefix = "34".parse().unwrap();
/// assert_eq!(value, 34.into());
///
/// let value: UnitPrefix = "1k".parse().unwrap();
/// assert_eq!(value, 1_000.into());
///
/// let value: UnitPrefix = "1 k".parse().unwrap();
/// assert_eq!(value, 1_000.into());
///
/// let value: UnitPrefix = "1M".parse().unwrap();
/// assert_eq!(value, 1_000_000.into());
///
/// let value: UnitPrefix = "1G".parse().unwrap();
/// assert_eq!(value, 1_000_000_000.into());
///
/// let value: UnitPrefix = "1Ki".parse().unwrap();
/// assert_eq!(value, 1_024.into());
///
/// let value: UnitPrefix = "1Mi".parse().unwrap();
/// assert_eq!(value, 1_048_576.into());
///
/// let value: UnitPrefix = "1Gi".parse().unwrap();
/// assert_eq!(value, 1_073_741_824.into());
///
/// // Case sensitive
/// let res = "1K".parse::<UnitPrefix>();
/// assert!(res.is_err());
///
/// let res = "x".parse::<UnitPrefix>();
/// assert!(res.is_err());
///
/// let res = "".parse::<UnitPrefix>();
/// assert!(res.is_err());
/// ```
#[derive(Debug, Clone, Copy, Eq, Ord)]
pub enum UnitPrefix {
    Unit(u64),
    Kilo(u64),
    Mega(u64),
    Giga(u64),
    Kibi(u64),
    Mebi(u64),
    Gibi(u64),
}

impl UnitPrefix {
    /// Returns the actual value with the prefix evaluated.
    ///
    /// ```rust
    /// # use condow_core::config::{UnitPrefix, Unit, Kilo};
    ///
    /// let value = UnitPrefix::Kilo(93).value();
    /// assert_eq!(value, 93_000);
    ///
    /// let value = UnitPrefix::Unit(93).value();
    /// assert_eq!(value, 93);
    /// ```
    pub fn value(self) -> u64 {
        match self {
            UnitPrefix::Unit(v) => v,
            UnitPrefix::Kilo(v) => Kilo(v).value(),
            UnitPrefix::Mega(v) => Mega(v).value(),
            UnitPrefix::Giga(v) => Giga(v).value(),
            UnitPrefix::Kibi(v) => Kibi(v).value(),
            UnitPrefix::Mebi(v) => Mebi(v).value(),
            UnitPrefix::Gibi(v) => Gibi(v).value(),
        }
    }
}

impl From<UnitPrefix> for u64 {
    fn from(m: UnitPrefix) -> Self {
        m.value()
    }
}

impl From<u64> for UnitPrefix {
    fn from(v: u64) -> Self {
        UnitPrefix::Unit(v)
    }
}

impl FromStr for UnitPrefix {
    type Err = AnyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if let Some(idx) = s.find(|c: char| c.is_alphabetic()) {
            if idx == 0 {
                bail!("'{}' needs digits", s)
            }

            let digits = from_utf8(&s.as_bytes()[..idx])?.trim();
            let unit = from_utf8(&s.as_bytes()[idx..])?.trim();

            let bytes = digits.parse::<u64>()?;

            match unit {
                "k" => Ok(UnitPrefix::Kilo(bytes)),
                "M" => Ok(UnitPrefix::Mega(bytes)),
                "G" => Ok(UnitPrefix::Giga(bytes)),
                "Ki" => Ok(UnitPrefix::Kibi(bytes)),
                "Mi" => Ok(UnitPrefix::Mebi(bytes)),
                "Gi" => Ok(UnitPrefix::Gibi(bytes)),
                s => bail!("invalid unit: '{}'", s),
            }
        } else {
            Ok(s.parse::<u64>().map(UnitPrefix::Unit)?)
        }
    }
}

impl PartialEq for UnitPrefix {
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }
}

impl PartialOrd for UnitPrefix {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value().partial_cmp(&other.value())
    }
}
