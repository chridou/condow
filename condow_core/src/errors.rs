/// Error types returned by Condow
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CondowError {
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

impl From<GetSizeError> for CondowError {
    fn from(dfe: GetSizeError) -> Self {
        match dfe {
            GetSizeError::NotFound(msg) => CondowError::NotFound(msg),
            GetSizeError::AccessDenied(msg) => CondowError::AccessDenied(msg),
            GetSizeError::Remote(msg) => CondowError::Remote(msg),
            GetSizeError::Io(msg) => CondowError::Io(msg),
            GetSizeError::Other(msg) => CondowError::Other(msg),
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
