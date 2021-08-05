use thiserror::Error;

use crate::{condow_client::DownloadRangeError, errors::IoError};
use bytes::Bytes;
use futures::stream::BoxStream;

mod chunk_stream;

pub use chunk_stream::*;

pub type BytesStream = BoxStream<'static, Result<Bytes, IoError>>;

/// The total number of bytes bytes in this stream
pub type TotalBytesHint = Option<usize>;

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("io error: {0}")]
    Io(IoError),
    #[error("{0}")]
    Other(String),
}

impl From<DownloadRangeError> for StreamError {
    fn from(dre: DownloadRangeError) -> Self {
        match dre {
            DownloadRangeError::Io(msg) => StreamError::Io(IoError(msg)),
            err => StreamError::Other(err.to_string()),
        }
    }
}
