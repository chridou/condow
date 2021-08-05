use thiserror::Error;

use crate::condow_client::{DownloadFullError, GetSizeError};

#[derive(Error, Debug)]
pub enum DownloadPartError {
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

#[derive(Error, Debug)]
pub enum DownloadFileError {
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

impl From<DownloadFileError> for DownloadPartError {
    fn from(dfe: DownloadFileError) -> Self {
        match dfe {
            DownloadFileError::NotFound(msg) => DownloadPartError::NotFound(msg),
            DownloadFileError::AccessDenied(msg) => DownloadPartError::AccessDenied(msg),
            DownloadFileError::Remote(msg) => DownloadPartError::Remote(msg),
            DownloadFileError::Io(msg) => DownloadPartError::Io(msg),
            DownloadFileError::Other(msg) => DownloadPartError::Other(msg),
        }
    }
}

impl From<DownloadPartError> for DownloadFileError {
    fn from(dfe: DownloadPartError) -> Self {
        match dfe {
            DownloadPartError::InvalidRange(msg) => DownloadFileError::Other(msg),
            DownloadPartError::NotFound(msg) => DownloadFileError::NotFound(msg),
            DownloadPartError::AccessDenied(msg) => DownloadFileError::AccessDenied(msg),
            DownloadPartError::Remote(msg) => DownloadFileError::Remote(msg),
            DownloadPartError::Io(msg) => DownloadFileError::Io(msg),
            DownloadPartError::Other(msg) => DownloadFileError::Other(msg),
        }
    }
}

impl From<DownloadFullError> for DownloadFileError {
    fn from(dfe: DownloadFullError) -> Self {
        match dfe {
            DownloadFullError::NotFound(msg) => DownloadFileError::NotFound(msg),
            DownloadFullError::AccessDenied(msg) => DownloadFileError::AccessDenied(msg),
            DownloadFullError::Remote(msg) => DownloadFileError::Remote(msg),
            DownloadFullError::Io(msg) => DownloadFileError::Io(msg),
            DownloadFullError::Other(msg) => DownloadFileError::Other(msg),
        }
    }
}

impl From<GetSizeError> for DownloadPartError {
    fn from(dfe: GetSizeError) -> Self {
        match dfe {
            GetSizeError::NotFound(msg) => DownloadPartError::NotFound(msg),
            GetSizeError::AccessDenied(msg) => DownloadPartError::AccessDenied(msg),
            GetSizeError::Remote(msg) => DownloadPartError::Remote(msg),
            GetSizeError::Io(msg) => DownloadPartError::Io(msg),
            GetSizeError::Other(msg) => DownloadPartError::Other(msg),
        }
    }
}

#[derive(Error, Debug)]
#[error("io error: {0}")]
pub struct IoError(pub String);
