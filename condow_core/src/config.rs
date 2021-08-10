use std::{
    str::{from_utf8, FromStr},
    time::Duration,
};

use anyhow::{bail, Error as AnyError};

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Config {
    pub part_size_bytes: PartSizeBytes,
    pub max_concurrency: MaxConcurrency,
    pub buffer_size: BufferSize,
    pub buffers_full_delay_ms: BuffersFullDelayMs,
    pub always_get_size: AlwaysGetSize,
}

impl Config {
    env_ctors!();

    pub fn part_size_bytes<T: Into<PartSizeBytes>>(mut self, part_size_bytes: T) -> Self {
        self.part_size_bytes = part_size_bytes.into();
        self
    }

    pub fn max_concurrency<T: Into<MaxConcurrency>>(mut self, max_concurrency: T) -> Self {
        self.max_concurrency = max_concurrency.into();
        self
    }

    pub fn buffer_size<T: Into<BufferSize>>(mut self, buffer_size: T) -> Self {
        self.buffer_size = buffer_size.into();
        self
    }

    pub fn buffers_full_delay_ms<T: Into<BuffersFullDelayMs>>(
        mut self,
        buffers_full_delay_ms: T,
    ) -> Self {
        self.buffers_full_delay_ms = buffers_full_delay_ms.into();
        self
    }

    pub fn always_get_size<T: Into<AlwaysGetSize>>(mut self, always_get_size: T) -> Self {
        self.always_get_size = always_get_size.into();
        self
    }

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
        PartSizeBytes::new(Mebi(2))
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

            let digits = from_utf8(&s.as_bytes()[0..idx])?.trim();
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
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct MaxConcurrency(usize, env="MAX_CONCURRENCY");
}

impl Default for MaxConcurrency {
    fn default() -> Self {
        MaxConcurrency(64)
    }
}

new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct BufferSize(usize, env="BUFFER_SIZE");
}

impl Default for BufferSize {
    fn default() -> Self {
        BufferSize(2)
    }
}

new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct AlwaysGetSize(bool, env="ALWAYS_GET_SIZE");
}

impl Default for AlwaysGetSize {
    fn default() -> Self {
        AlwaysGetSize(true)
    }
}

new_type! {
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mega(pub usize);

impl From<Mega> for usize {
    fn from(m: Mega) -> Self {
        m.0 * 1_000_000
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Kilo(pub usize);

impl From<Kilo> for usize {
    fn from(m: Kilo) -> Self {
        m.0 * 1_000
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Giga(pub usize);

impl From<Giga> for usize {
    fn from(m: Giga) -> Self {
        m.0 * 1_000_000_000
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Kibi(pub usize);

impl From<Kibi> for usize {
    fn from(m: Kibi) -> Self {
        m.0 * 1_024
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mebi(pub usize);

impl From<Mebi> for usize {
    fn from(m: Mebi) -> Self {
        m.0 * 1_024 * 1_024
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gibi(pub usize);

impl From<Gibi> for usize {
    fn from(m: Gibi) -> Self {
        m.0 * 1_024 * 1_024 * 1_024
    }
}
