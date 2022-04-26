//! Download request

use std::sync::Arc;

use crate::{
    condow_client::{CondowClient, NoLocation},
    errors::CondowError,
    machinery::{self, ProbeInternal},
    probe::Probe,
    ChunkStream, Condow, DownloadRange, GetSizeMode, PartStream,
};

/// A request for a download where the location is not yet known
///
/// This can only download if the type of the location is [NoLocation].
pub struct RequestNoLocation<C> {
    condow: Condow<C>,
    probe: Option<Arc<dyn Probe>>,
    range: DownloadRange,
    get_size_mode: GetSizeMode,
}

impl<C> RequestNoLocation<C>
where
    C: CondowClient,
{
    pub fn new(condow: Condow<C>) -> Self {
        Self {
            condow,
            probe: None,
            range: (..).into(),
            get_size_mode: GetSizeMode::Default,
        }
    }

    /// Specify the location to download the Blob from
    pub fn at<L: Into<C::Location>>(self, location: L) -> Request<C> {
        Request {
            condow: self.condow,
            location: location.into(),
            probe: self.probe,
            range: self.range,
            get_size_mode: self.get_size_mode,
        }
    }

    /// Specify the range to download
    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.range = range.into();
        self
    }

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.probe = Some(probe);
        self
    }

    /// Explicitly set the condition on when to query for the Blob size
    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.get_size_mode = get_size_mode;
        self
    }
}

impl<C> RequestNoLocation<C>
where
    C: CondowClient<Location = NoLocation>,
{
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
pub struct Request<C>
where
    C: CondowClient,
{
    condow: Condow<C>,
    location: C::Location,
    probe: Option<Arc<dyn Probe>>,
    range: DownloadRange,
    get_size_mode: GetSizeMode,
}

impl<C> Request<C>
where
    C: CondowClient,
{
    /// Specify the location to download the Blob from
    pub fn at<L: Into<C::Location>>(mut self, location: L) -> Self {
        self.location = location.into();
        self
    }

    /// Specify the range to download
    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.range = range.into();
        self
    }

    /// Attach a [Probe] to the download
    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.probe = Some(probe);
        self
    }

    /// Explicitly set the condition on when to query for the Blob size
    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.get_size_mode = get_size_mode;
        self
    }

    /// Download as a [ChunkStream]
    pub async fn download_chunks(self) -> Result<ChunkStream, CondowError> {
        let probe = match (
            self.probe,
            self.condow
                .probe_factory
                .as_ref()
                .map(|f| f.make(&self.location)),
        ) {
            (None, None) => ProbeInternal::Off,
            (Some(req), None) => ProbeInternal::One(req),
            (None, Some(fac)) => ProbeInternal::One(fac),
            (Some(req), Some(fac)) => ProbeInternal::Two(req, fac),
        };

        machinery::download_range(
            self.condow,
            self.location,
            self.range,
            self.get_size_mode,
            probe,
        )
        .await
    }

    /// Download as a [PartStream]
    pub async fn download(self) -> Result<PartStream<ChunkStream>, CondowError> {
        PartStream::from_chunk_stream(self.download_chunks().await?)
    }
}
