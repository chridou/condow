//! Configuration items
//!
//! Many of the fields of [Config] are
//! implemented with the new type pattern.
//! This is mainly to allow them to be
//! initialized from the envirinment.
use std::{
    str::{from_utf8, FromStr},
    time::Duration,
};

use anyhow::{bail, Error as AnyError};

/// A configuration for [Condow](super::Condow).
#[derive(Debug, Clone, Default, Eq, PartialEq)]
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
    /// Size of the buffer for each download task.
    ///
    /// If set to 0 (not advised) there will be now buffer at all.
    /// This should be configured so that none of the tasks runs ever empty
    ///
    /// Default is 2
    pub buffer_size: BufferSize,
    /// If all buffers of all download tasks are full, this is the time
    /// to pause until the next attempt.
    ///
    /// Default is 10ms
    pub buffers_full_delay_ms: BuffersFullDelayMs,
    /// If `true` [Condow](super::Condow) will also request the
    /// size information of a BLOB to verify the range supplied
    /// by a user.
    ///
    /// The default is `true`.
    pub always_get_size: AlwaysGetSize,
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

    /// Set the size of the buffer for each download task.
    pub fn buffer_size<T: Into<BufferSize>>(mut self, buffer_size: T) -> Self {
        self.buffer_size = buffer_size.into();
        self
    }

    /// Set the delay in case all task buffers are full before a retry
    /// to enqueue the next downlod part is made.
    pub fn buffers_full_delay_ms<T: Into<BuffersFullDelayMs>>(
        mut self,
        buffers_full_delay_ms: T,
    ) -> Self {
        self.buffers_full_delay_ms = buffers_full_delay_ms.into();
        self
    }

    /// Set whether a size request should always be made
    pub fn always_get_size<T: Into<AlwaysGetSize>>(mut self, always_get_size: T) -> Self {
        self.always_get_size = always_get_size.into();
        self
    }

    /// Validate this [Config]
    pub fn validated(self) -> Result<Self, AnyError> {
        if self.max_concurrency.0 == 0 {
            bail!("'max_concurrency' must not be 0");
        }

        if self.part_size_bytes.0 == 0 {
            bail!("'part_size_bytes' must not be 0");
        }

        Ok(self)
    }

    fn fill_from_env_prefixed_internal<T: AsRef<str>>(
        &mut self,
        prefix: T,
    ) -> Result<(), AnyError> {
        if let Some(part_size_bytes) = PartSizeBytes::try_from_env_prefixed(prefix.as_ref())? {
            self.part_size_bytes = part_size_bytes;
        }
        if let Some(max_concurrency) = MaxConcurrency::try_from_env_prefixed(prefix.as_ref())? {
            self.max_concurrency = max_concurrency;
        }
        if let Some(buffer_size) = BufferSize::try_from_env_prefixed(prefix.as_ref())? {
            self.buffer_size = buffer_size;
        }
        if let Some(buffers_full_delay_ms) =
            BuffersFullDelayMs::try_from_env_prefixed(prefix.as_ref())?
        {
            self.buffers_full_delay_ms = buffers_full_delay_ms;
        }
        if let Some(always_get_size) = AlwaysGetSize::try_from_env_prefixed(prefix.as_ref())? {
            self.always_get_size = always_get_size;
        }

        Ok(())
    }
}

/// Size of the parts in bytes a download is split into
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PartSizeBytes(usize);

impl PartSizeBytes {
    pub fn new<T: Into<usize>>(part_size_bytes: T) -> Self {
        Self(part_size_bytes.into())
    }

    env_funs!("PART_SIZE_BYTES");
}

impl Default for PartSizeBytes {
    fn default() -> Self {
        PartSizeBytes::new(Mebi(4))
    }
}

impl From<usize> for PartSizeBytes {
    fn from(v: usize) -> Self {
        PartSizeBytes(v)
    }
}

impl From<PartSizeBytes> for usize {
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
        let s = s.trim();
        if let Some(idx) = s.find(|c: char| c.is_alphabetic()) {
            if idx == 0 {
                bail!("'{}' needs digits", s)
            }

            let digits = from_utf8(&s.as_bytes()[..idx])?.trim();
            let unit = from_utf8(&s.as_bytes()[idx + 1..])?.trim();

            let bytes = digits.parse::<usize>()?;

            match unit {
                "k" => Ok(Kilo(bytes).into()),
                "M" => Ok(Mega(bytes).into()),
                "G" => Ok(Giga(bytes).into()),
                "Ki" => Ok(Kibi(bytes).into()),
                "Mi" => Ok(Mebi(bytes).into()),
                "Gi" => Ok(Gibi(bytes).into()),
                s => bail!("invaid unit: '{}'", s),
            }
        } else {
            Ok(s.parse()?)
        }
    }
}

new_type! {
    #[doc="Maximum concurrency of a single download"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct MaxConcurrency(usize, env="MAX_CONCURRENCY");
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
    #[doc="Behaviour for get size requests"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct AlwaysGetSize(bool, env="ALWAYS_GET_SIZE");
}

impl Default for AlwaysGetSize {
    fn default() -> Self {
        AlwaysGetSize(true)
    }
}

new_type! {
    #[doc="Time to wait for download buffers when all were full in ms"]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct BuffersFullDelayMs(u64, env="BUFFERS_FULL_DELAY_MS");
}

impl Default for BuffersFullDelayMs {
    fn default() -> Self {
        Self(10)
    }
}

impl From<BuffersFullDelayMs> for Duration {
    fn from(m: BuffersFullDelayMs) -> Self {
        Duration::from_millis(m.0)
    }
}

/// Multiplies by 1_000_000 when converted to a usize
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mega(pub usize);

impl From<Mega> for usize {
    fn from(m: Mega) -> Self {
        m.0 * 1_000_000
    }
}

/// Multiplies by 1_000 when converted to a usize
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Kilo(pub usize);

impl From<Kilo> for usize {
    fn from(m: Kilo) -> Self {
        m.0 * 1_000
    }
}

/// Multiplies by 1_000_000_000 when converted to a usize
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Giga(pub usize);

impl From<Giga> for usize {
    fn from(m: Giga) -> Self {
        m.0 * 1_000_000_000
    }
}

/// Multiplies by 1_024 when converted to a usize
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Kibi(pub usize);

impl From<Kibi> for usize {
    fn from(m: Kibi) -> Self {
        m.0 * 1_024
    }
}
/// Multiplies by 1_048_576 when converted to a usize
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mebi(pub usize);

impl From<Mebi> for usize {
    fn from(m: Mebi) -> Self {
        m.0 * 1_024 * 1_024
    }
}

/// Multiplies by 1_073_741_824 when converted to a usize
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gibi(pub usize);

impl From<Gibi> for usize {
    fn from(m: Gibi) -> Self {
        m.0 * 1_024 * 1_024 * 1_024
    }
}
