use crate::errors::IoError;
use bytes::Bytes;
use futures::stream::BoxStream;

mod chunk_stream;
mod parts_stream;

pub use chunk_stream::*;
pub use parts_stream::*;

/// A stream of [Bytes]
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

    /// An exact number of bytes will be returned.
    pub fn new_exact(bytes: usize) -> Self {
        Self(bytes, Some(bytes))
    }

    /// Create a hint of `min=0` and `max=bytes` bytes
    pub fn new_at_max(bytes: usize) -> Self {
        Self(0, Some(bytes))
    }

    /// Creates a new hint which gives no hint at all.
    ///
    /// `(0, None)`
    pub fn new_no_hint() -> Self {
        Self(0, None)
    }

    /// Returns the lower bound
    pub fn lower_bound(&self) -> usize {
        self.0
    }

    /// A `None` here means that either there is no known upper bound,
    /// or the upper bound is larger than usize.
    pub fn upper_bound(&self) -> Option<usize> {
        self.1
    }

    /// Returns true if this hint is an exact hint.
    ///
    /// This means that the lower bound must equal the upper bound: `(a, Some(a))`.
    pub fn is_exact(&self) -> bool {
        if let Some(upper) = self.1 {
            upper == self.0
        } else {
            false
        }
    }

    /// Returns the exact bytes if size hint specifies an exact amount of bytes.
    ///
    /// This means that the lower bound must equal the upper bound: `(a, Some(a))`.
    pub fn exact(&self) -> Option<usize> {
        if self.is_exact() {
            self.1
        } else {
            None
        }
    }

    /// Bytes have been send and `by` less will be received from now on
    pub fn reduce_by(&mut self, by: usize) {
        if by == 0 {
            return;
        }

        match self {
            BytesHint(0, None) => {}
            BytesHint(min, None) => {
                if *min >= by {
                    *min -= by;
                } else {
                    *self = BytesHint::new_no_hint()
                }
            }
            BytesHint(0, Some(max)) => {
                if *max >= by {
                    *max -= by;
                } else {
                    *self = BytesHint::new_no_hint()
                }
            }
            BytesHint(min, Some(max)) => {
                if *min >= by {
                    *min -= by;
                } else {
                    *min = 0;
                }

                if *max >= by {
                    *max -= by;
                } else {
                    self.1 = None
                }
            }
        }
    }

    /// Turns this into the inner tuple
    pub fn into_inner(self) -> (usize, Option<usize>) {
        (self.lower_bound(), self.upper_bound())
    }
}
