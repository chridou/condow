use futures::future::BoxFuture;

use crate::{
    errors::{DownloadRangeError, GetSizeError},
    streams::{BytesHint, BytesStream},
    DownloadRange,
};

pub trait CondowClient: Clone + Send + Sync + 'static {
    type Location: std::fmt::Debug + Clone + Send + Sync + 'static;

    fn get_size(&self, location: Self::Location)
        -> BoxFuture<'static, Result<usize, GetSizeError>>;
    fn download(
        &self,
        _location: Self::Location,
        range: DownloadRange,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), DownloadRangeError>>;
}
