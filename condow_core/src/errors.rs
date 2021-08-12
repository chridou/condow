use thiserror::Error;

#[derive(Error, Debug)]
pub enum DownloadError {
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

impl From<GetSizeError> for DownloadError {
    fn from(dfe: GetSizeError) -> Self {
        match dfe {
            GetSizeError::NotFound(msg) => DownloadError::NotFound(msg),
            GetSizeError::AccessDenied(msg) => DownloadError::AccessDenied(msg),
            GetSizeError::Remote(msg) => DownloadError::Remote(msg),
            GetSizeError::Io(msg) => DownloadError::Io(msg),
            GetSizeError::Other(msg) => DownloadError::Other(msg),
        }
    }
}

#[derive(Error, Debug)]
pub enum StreamError {
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

impl From<DownloadError> for StreamError {
    fn from(dre: DownloadError) -> Self {
        match dre {
            DownloadError::InvalidRange(msg) => StreamError::InvalidRange(msg),
            DownloadError::NotFound(msg) => StreamError::NotFound(msg),
            DownloadError::AccessDenied(msg) => StreamError::AccessDenied(msg),
            DownloadError::Remote(msg) => StreamError::Remote(msg),
            DownloadError::Io(msg) => StreamError::Io(msg),
            DownloadError::Other(msg) => StreamError::Other(msg),
        }
    }
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
#[error("io error: {0}")]
pub struct IoError(pub String);
