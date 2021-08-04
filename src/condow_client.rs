use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream};
use thiserror::Error;

pub type BytesStream = BoxStream<'static, Result<Bytes, IoError>>;

pub trait CondowClient {
    type Location: std::fmt::Display + Clone;

    fn get_size(&self, location: Self::Location)
        -> BoxFuture<'static, Result<usize, GetSizeError>>;
    fn download_range(
        location: Self::Location,
        from_inclusive: usize,
        to_inclusive: usize,
    ) -> BoxFuture<'static, Result<BytesStream, DownloadRangeError>>;
    fn download_full(
        &self,
        location: Self::Location,
    ) -> BoxFuture<'static, Result<BytesStream, DownloadFullError>>;
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
pub enum DownloadRangeError {
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
pub enum DownloadFullError {
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
#[error("io error: {0}")]
pub struct IoError(String);
