use futures::future::BoxFuture;
use thiserror::Error;

use crate::{DownloadRange, streams::{BytesStream, TotalBytesHint}};

pub trait CondowClient: Clone + Send + Sync + 'static {
    type Location: std::fmt::Display + Clone + Send + Sync + 'static;

    fn get_size(&self, location: Self::Location)
        -> BoxFuture<'static, Result<usize, GetSizeError>>;
    fn download(
        &self,
        _location: Self::Location,
        range: DownloadRange,
    ) -> BoxFuture<'static, Result<(BytesStream, TotalBytesHint), ClientDownloadError>>;
  
}

#[derive(Error, Debug)]
pub enum GetSizeError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("error: {0}")]
    Remote(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("error: {0}")]
    Other(String),
}

#[derive(Error, Debug)]
pub enum ClientDownloadError {
    #[error("invalid range: {0}")]
    InvalidRange(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("access denied: {0}")]
    AccessDenied(String),
    #[error("error: {0}")]
    Remote(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("error: {0}")]
    Other(String),
}