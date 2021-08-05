use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub part_size: PartSize,
    pub max_concurrent: MaxConcurrent,
    pub buffer_size: BufferSize,
    pub buffers_full_delay: BuffersFullDelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartSize(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxConcurrent(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferSize(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuffersFullDelay(pub Duration);
