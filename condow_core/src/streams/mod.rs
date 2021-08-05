use thiserror::Error;

use crate::errors::IoError;
use bytes::Bytes;
use futures::stream::BoxStream;

mod chunk_stream;

pub use chunk_stream::ChunkStream;

pub type BytesStream = BoxStream<'static, Result<Bytes, IoError>>;


/// The total number of bytes bytes in this stream
pub type TotalBytesHint = Option<usize>;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("{0}")]
    Io(IoError),
    #[error("io error: {0}")]
    Other(String),
}
