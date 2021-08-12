use futures::future::BoxFuture;

use crate::{
    errors::{DownloadError, GetSizeError},
    streams::{BytesHint, BytesStream},
    InclusiveRange,
};

#[derive(Debug, Copy, Clone)]
pub enum DownloadSpec {
    Complete,
    Range(InclusiveRange),
}
pub trait CondowClient: Clone + Send + Sync + 'static {
    type Location: std::fmt::Debug + Clone + Send + Sync + 'static;

    fn get_size(&self, location: Self::Location)
        -> BoxFuture<'static, Result<usize, GetSizeError>>;
    fn download(
        &self,
        location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), DownloadError>>;
}
