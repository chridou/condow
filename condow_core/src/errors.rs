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

    pub fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
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
    /// An invalid range was encountered.
    ///
    /// Errors with this kind are **not retryable**
    InvalidRange,
    /// The BLOB could not be found at a given location.
    ///
    /// Errors with this kind are **not retryable**
    NotFound,
    /// Access was denied to the BLOB at a given location.
    ///
    /// Could be "unauthenticated", "unauthorized" or anything else
    /// regarding credentials
    ///
    /// Errors with this kind are **not retryable**
    AccessDenied,
    /// The resource providing the BLOB encountered an error
    ///
    /// Errors with this kind are **retryable**
    Remote,
    /// Something went wrong with our data "on the wire"
    ///
    /// Errors with this kind are **retryable**
    Io,
    /// Anything else which does not fall under one of the other categories
    ///
    /// Errors with this kind are **not retryable**
    Other,
}

impl CondowErrorKind {
    pub fn is_retryable(self) -> bool {
        use CondowErrorKind::*;

        match self {
            InvalidRange => false,
            NotFound => false,
            AccessDenied => false,
            Remote => true,
            Io => true,
            Other => false,
        }
    }
}

impl From<CondowErrorKind> for CondowError {
    fn from(error_kind: CondowErrorKind) -> Self {
        CondowError::new(format!("An error occurred: {:?}", error_kind), error_kind)
    }
}

impl From<std::io::Error> for CondowError {
    fn from(io: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match io.kind() {
            ErrorKind::NotFound => CondowError::new_not_found("not found"),
            ErrorKind::PermissionDenied => CondowError::new_access_denied("permission denied"),
            _ => CondowError::new_io("io error"),
        }
        .with_source(io)
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

/// Utility function to convert an error HTTP response to a CondowError.
pub fn http_status_to_error(status_code: u16, status_str: &str, is_server_error: bool, body: &[u8]) -> CondowError {
    // Ideally this fn should take the StatusCode from http crate,
    // but that would make multiple crates depend on the same http crate, possibly with different versions.
    let message = if let Ok(body_str) = std::str::from_utf8(body) {
        body_str
    } else {
        "<<< response body is not valid UTF-8 >>>"
    };
    let message = format!("{} - {}", status_str, message);
    match status_code {
        404 => CondowError::new_not_found(message),
        401 | 403 => CondowError::new_access_denied(message),
        _ => {
            if is_server_error {
                CondowError::new_remote(message)
            } else {
                CondowError::new_other(message)
            }
        }
    }
}
