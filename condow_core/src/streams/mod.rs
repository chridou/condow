use crate::errors::IoError;
use bytes::Bytes;
use futures::stream::BoxStream;

mod chunk_stream;

pub use chunk_stream::*;

pub type BytesStream = BoxStream<'static, Result<Bytes, IoError>>;

/// Returns the bounds on the remaining bytes of the stream.
///
/// Specifically, `bytes_hint()` returns a tuple where the first element is
/// the lower bound, and the second element is the upper bound.
///
/// The second half of the tuple that is returned is an `Option<usize>`.
/// A None here means that either there is no known upper bound,
/// or the upper bound is larger than usize.
#[derive(Debug, Copy, Clone)]
pub struct BytesHint(usize, Option<usize>);

impl BytesHint {
    pub fn new(lower_bound: usize, upper_bound: Option<usize>) -> Self {
        BytesHint(lower_bound, upper_bound)
    }

    pub fn exact(bytes: usize) -> Self {
        Self(bytes, Some(bytes))
    }

    pub fn at_max(bytes: usize) -> Self {
        Self(0, Some(bytes))
    }

    pub fn no_hint() -> Self {
        Self(0, None)
    }

    pub fn lower_bound(&self) -> usize {
        self.0
    }

    /// A `None` here means that either there is no known upper bound,
    /// or the upper bound is larger than usize.
    pub fn upper_bound(&self) -> Option<usize> {
        self.1
    }

    pub fn tuple(self) -> (usize, Option<usize>) {
        (self.lower_bound(), self.upper_bound())
    }
}
