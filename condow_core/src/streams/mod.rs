//! Stream implememtations used by Condow
use std::fmt;

use crate::errors::{CondowError, IoError};
use bytes::Bytes;
use futures::stream::BoxStream;

mod chunk_stream;
mod ordered_chunk_stream;

pub use chunk_stream::*;
pub use ordered_chunk_stream::*;

/// A stream of [Bytes] (chunks) where there can be an error for each chunk of bytes
pub type BytesStream = BoxStream<'static, Result<Bytes, IoError>>;

/// The type of the elements returned by a [ChunkStream]
pub type ChunkStreamItem = Result<Chunk, CondowError>;

/// A chunk belonging to a downloaded part
///
/// All chunks of a part will have the correct order
/// for a part with the same `part_index` but the chunks
/// of different parts can be intermingled due
/// to the nature of a concurrent download.
///
/// If a download should be processed (bytewise) sequentially
/// the chunks need to be ordered (see [OrderedChunkStream]).
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Index of the part this chunk belongs to
    pub part_index: u64,
    /// Index of the chunk within the part
    ///
    /// For each new part this must start with zero.
    pub chunk_index: usize,
    /// Offset of the chunk within the (original) BLOB
    ///
    /// This value can be used to reconstruct a BLOB consisting of multiple
    /// downloads. E.g. if there is a buffer which has the size of the original
    /// BLOB this would be the offset to put the bytes of this chunk.
    pub blob_offset: u64,
    /// Offset of the chunk within the downloaded range
    ///
    /// This can be used to reconstruct the range of the BLOB which was downloaded.
    /// E.g. if there is a buffer which has the size of the downloaded range
    /// this would be the offset to put the bytes of this chunk.
    pub range_offset: u64,
    /// The bytes
    pub bytes: Bytes,
    /// Bytes left in following chunks of the same part. If 0 this is the last chunk of the part.
    ///
    /// This is the bytes left excluding the number of bytes of the current chunk.
    pub bytes_left: u64,
}

impl Chunk {
    /// Returns `true` if this is the last chunk of the part
    pub fn is_last(&self) -> bool {
        self.bytes_left == 0
    }

    /// Returns the number of bytes in this chunk
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns `true` if there are no bytes in this chunk.
    ///
    /// This should not happen since we would not expect
    /// "no bytes" being sent over the network.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Returns the bounds on the remaining bytes of the stream.
///
/// Specifically, `bytes_hint()` returns a tuple where the first element is
/// the lower bound, and the second element is the upper bound.
///
/// The second half of the tuple that is returned is an `Option<u64>`.
/// A None here means that either there is no known upper bound,
/// or the upper bound is larger than `u64`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BytesHint(u64, Option<u64>);

impl BytesHint {
    pub fn new(lower_bound: u64, upper_bound: Option<u64>) -> Self {
        BytesHint(lower_bound, upper_bound)
    }

    /// An exact number of bytes will be returned.
    pub fn new_exact(bytes: u64) -> Self {
        Self(bytes, Some(bytes))
    }

    /// Create a hint of `min=0` and `max=bytes` bytes
    pub fn new_at_max(bytes: u64) -> Self {
        Self(0, Some(bytes))
    }

    /// Creates a new hint which gives no hint at all.
    ///
    /// `(0, None)`
    pub fn new_no_hint() -> Self {
        Self(0, None)
    }

    /// Returns the lower bound
    pub fn lower_bound(&self) -> u64 {
        self.0
    }

    /// A `None` here means that either there is no known upper bound,
    /// or the upper bound is larger than usize.
    pub fn upper_bound(&self) -> Option<u64> {
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
    pub fn exact(&self) -> Option<u64> {
        if self.is_exact() {
            self.1
        } else {
            None
        }
    }

    /// Bytes have been send and `by` less will be received from now on
    pub fn reduce_by(&mut self, by: u64) {
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

    pub fn combine(self, other: BytesHint) -> BytesHint {
        let (me_lower, me_upper) = self.into_inner();
        let (other_lower, other_upper) = other.into_inner();

        let lower_bound = me_lower + other_lower;

        let upper_bound = match (me_upper, other_upper) {
            (Some(a), Some(b)) => Some(a + b),
            (Some(_), None) => None,
            (None, Some(_)) => None,
            (None, None) => None,
        };

        BytesHint::new(lower_bound, upper_bound)
    }

    /// Turns this into the inner tuple
    pub fn into_inner(self) -> (u64, Option<u64>) {
        (self.lower_bound(), self.upper_bound())
    }
}

impl fmt::Display for BytesHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (lower, upper) = self.into_inner();

        match upper {
            Some(upper) => write!(f, "[{}..{}]", lower, upper),
            None => write!(f, "[{}..?[", lower),
        }
    }
}
