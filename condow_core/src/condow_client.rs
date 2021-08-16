//! Adapter for [crate::Condow] to access BLOBs to be downloaded
use futures::future::BoxFuture;

use crate::{
    errors::{CondowError, GetSizeError},
    streams::{BytesHint, BytesStream},
    InclusiveRange,
};

/// Specifies whether a whole BLOB or part of it should be downloaded
#[derive(Debug, Copy, Clone)]
pub enum DownloadSpec {
    /// Download the complete BLOB
    Complete,
    /// Download part of the BLOB given by an [InclusiveRange]
    Range(InclusiveRange),
}

impl DownloadSpec {
    /// Returns a value for an  `HTTP-Range` header with bytes as the unit
    /// if the variant is [DownloadSpec::Range]
    pub fn http_range_value(&self) -> Option<String> {
        match self {
            DownloadSpec::Complete => None,
            DownloadSpec::Range(r) => Some(r.http_range_value()),
        }
    }
}

/// A client to some service or other resource which supports
/// partial downloads
///
/// This is an adapter trait
pub trait CondowClient: Clone + Send + Sync + 'static {
    type Location: std::fmt::Debug + Clone + Send + Sync + 'static;

    /// Returns the size of the BLOB at the given location
    fn get_size(&self, location: Self::Location)
        -> BoxFuture<'static, Result<usize, GetSizeError>>;

    /// Download a BLOB or part of a BLOB from the given location as specified by the [DownloadSpec]
    ///
    /// A valid [BytesHint] must be returned alongside the stream.
    /// A concurrent download will fail if the [BytesHint] does not match
    /// the number of bytes requested by a [DownloadSpec::Range].
    fn download(
        &self,
        location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>;
}
