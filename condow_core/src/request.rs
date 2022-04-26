//! Download request

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::{
    condow_client::NoLocation, errors::CondowError, probe::Probe, ChunkStream, DownloadRange,
    GetSizeMode, PartStream,
};

type DownloadFn<L> = Box<
    dyn FnOnce(L, Params) -> BoxFuture<'static, Result<ChunkStream, CondowError>> + Send + 'static,
>;

/// A request for a download where the location is not yet known
///
/// This can only download if the type of the location is [NoLocation].
pub struct RequestNoLocation<L> {
    download_fn: DownloadFn<L>,
    params: Params,
}

impl<L> RequestNoLocation<L> {
    pub(crate) fn new<F>(download_fn: F) -> Self
    where
        F: FnOnce(L, Params) -> BoxFuture<'static, Result<ChunkStream, CondowError>>
            + Send
            + 'static,
    {
        Self {
            download_fn: Box::new(download_fn),
            params: Params {
                probe: None,
                range: (..).into(),
                get_size_mode: GetSizeMode::Default,
            },
        }
    }

    /// Specify the location to download the Blob from
    pub fn at<LL: Into<L>>(self, location: LL) -> Request<L> {
        Request {
            download_fn: self.download_fn,
            location: location.into(),
            params: self.params,
        }
    }

    /// Specify the range to download
    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.params.range = range.into();
        self
    }

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.params.probe = Some(probe);
        self
    }

    /// Explicitly set the condition on when to query for the Blob size
    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.params.get_size_mode = get_size_mode;
        self
    }
}

impl RequestNoLocation<NoLocation> {
    /// Download as a [ChunkStream]
    pub async fn download_chunks(self) -> Result<ChunkStream, CondowError> {
        self.at(NoLocation).download_chunks().await
    }

    /// Download as a [PartStream]
    pub async fn download(self) -> Result<PartStream<ChunkStream>, CondowError> {
        self.at(NoLocation).download().await
    }
}

/// A request for a download from a specific location
pub struct Request<L> {
    download_fn: DownloadFn<L>,
    location: L,
    params: Params,
}

impl<L> Request<L> {
    /// Specify the location to download the Blob from
    pub fn at<LL: Into<L>>(mut self, location: LL) -> Self {
        self.location = location.into();
        self
    }

    /// Specify the range to download
    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.params.range = range.into();
        self
    }

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.params.probe = Some(probe);
        self
    }

    /// Explicitly set the condition on when to query for the Blob size
    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.params.get_size_mode = get_size_mode;
        self
    }

    /// Download as a [ChunkStream]
    pub async fn download_chunks(self) -> Result<ChunkStream, CondowError> {
        (self.download_fn)(self.location, self.params).await
    }

    /// Download as a [PartStream]
    pub async fn download(self) -> Result<PartStream<ChunkStream>, CondowError> {
        PartStream::from_chunk_stream(self.download_chunks().await?)
    }
}

pub(crate) struct Params {
    pub probe: Option<Arc<dyn Probe>>,
    pub range: DownloadRange,
    pub get_size_mode: GetSizeMode,
}