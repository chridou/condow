use std::{str::FromStr, time::Duration};

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Config {
    pub part_size_bytes: PartSizeBytes,
    pub max_concurrency: MaxConcurrency,
    pub buffer_size: BufferSize,
    pub buffers_full_delay: BuffersFullDelay,
}

impl Config {
    pub fn part_size_bytes<T: Into<PartSizeBytes>>(mut self, v: T) -> Self {
        self.part_size_bytes = v.into();
        self
    }

    pub fn max_concurrency<T: Into<MaxConcurrency>>(mut self, v: T) -> Self {
        self.max_concurrency = v.into();
        self
    }

    pub fn buffer_size<T: Into<BufferSize>>(mut self, v: T) -> Self {
        self.buffer_size = v.into();
        self
    }

    pub fn buffers_full_delay<T: Into<BuffersFullDelay>>(mut self, v: T) -> Self {
        self.buffers_full_delay = v.into();
        self
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
        todo!()
    }
}

new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct MaxConcurrency(usize);
}

impl Default for MaxConcurrency {
    fn default() -> Self {
        MaxConcurrency(64)
    }
}

new_type! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub copy struct BufferSize(usize);
}

impl Default for BufferSize {
    fn default() -> Self {
        BufferSize(2)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuffersFullDelay(pub Duration);

impl BuffersFullDelay {
    env_funs!("BUFFERS_FULL_DELAY_MS");

    pub fn into_inner(self) -> Duration {
        self.0
    }
}

impl Default for BuffersFullDelay {
    fn default() -> Self {
        BuffersFullDelay(Duration::from_millis(10))
    }
}

impl FromStr for BuffersFullDelay {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl From<Duration> for BuffersFullDelay {
    fn from(d: Duration) -> Self {
        BuffersFullDelay(d)
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
