//! Error types returned by Condow
use std::fmt;

use thiserror::Error;

/// The error type used by `condow`
///
/// Further information is encoded with [CondowErrorKind].
#[derive(Error, Debug)]
pub struct CondowError {
    msg: String,
    #[source]
    source: Option<anyhow::Error>,
    kind: CondowErrorKind,
}

impl CondowError {
    pub fn new<T: Into<String>>(msg: T, kind: CondowErrorKind) -> Self {
        Self {
            msg: msg.into(),
            source: None,
            kind,
        }
    }
    pub fn new_invalid_range<T: Into<String>>(msg: T) -> Self {
        Self::new(msg, CondowErrorKind::InvalidRange)
    }

    pub fn new_not_found<T: Into<String>>(msg: T) -> Self {
        Self::new(msg, CondowErrorKind::NotFound)
    }

    pub fn new_access_denied<T: Into<String>>(msg: T) -> Self {
        Self::new(msg, CondowErrorKind::AccessDenied)
    }

    pub fn new_remote<T: Into<String>>(msg: T) -> Self {
        Self::new(msg, CondowErrorKind::Remote)
    }

    pub fn new_io<T: Into<String>>(msg: T) -> Self {
        Self::new(msg, CondowErrorKind::Io)
    }

    pub fn new_other<T: Into<String>>(msg: T) -> Self {
        Self::new(msg, CondowErrorKind::Other)
    }

    pub fn with_source<E: Into<anyhow::Error>>(mut self, err: E) -> Self {
        self.source = Some(err.into());
        self
    }

    pub fn msg(&self) -> &str {
        &self.msg
    }

    pub fn kind(&self) -> CondowErrorKind {
        self.kind
    }
}

impl fmt::Display for CondowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

/// Specifies the kind of a [CondowError]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CondowErrorKind {
    InvalidRange,
    NotFound,
    AccessDenied,
    Remote,
    Io,
    Other,
}

impl From<std::io::Error> for CondowError {
    fn from(io: std::io::Error) -> Self {
        CondowError::new_io("io error").with_source(io)
    }
}

impl From<IoError> for CondowError {
    fn from(io: IoError) -> Self {
        CondowError::new_io(io.0)
    }
}

#[derive(Error, Debug)]
#[error("io error: {0}")]
pub struct IoError(pub String);

impl From<std::io::Error> for IoError {
    fn from(io: std::io::Error) -> Self {
        IoError(io.to_string())
    }
}
