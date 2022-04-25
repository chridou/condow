use std::sync::Arc;

use crate::{
    condow_client::{CondowClient, NoLocation},
    errors::CondowError,
    machinery,
    probe::Probe,
    ChunkStream, Condow, DownloadRange, GetSizeMode, PartStream,
};

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

    pub fn location<L: Into<C::Location>>(self, location: L) -> Request<C> {
        Request {
            condow: self.condow,
            location: location.into(),
            probe: self.probe,
            range: self.range,
            get_size_mode: self.get_size_mode,
        }
    }

    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.range = range.into();
        self
    }

    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.probe = Some(probe);
        self
    }

    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.get_size_mode = get_size_mode;
        self
    }
}

impl<C> RequestNoLocation<C>
where
    C: CondowClient<Location = NoLocation>,
{
    pub async fn download_chunks(self) -> Result<ChunkStream, CondowError> {
        self.location(NoLocation).download_chunks().await
    }

    pub async fn download(self) -> Result<PartStream<ChunkStream>, CondowError> {
        self.location(NoLocation).download().await
    }
}

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
    pub fn location<L: Into<C::Location>>(mut self, location: L) -> Self {
        self.location = location.into();
        self
    }

    pub fn range<DR: Into<DownloadRange>>(mut self, range: DR) -> Self {
        self.range = range.into();
        self
    }

    pub fn probe(mut self, probe: Arc<dyn Probe>) -> Self {
        self.probe = Some(probe);
        self
    }

    pub fn get_size_mode(mut self, get_size_mode: GetSizeMode) -> Self {
        self.get_size_mode = get_size_mode;
        self
    }

    pub async fn download_chunks(self) -> Result<ChunkStream, CondowError> {
        machinery::download_range(
            self.condow,
            self.location,
            self.range,
            self.get_size_mode,
            self.probe.into(),
        )
        .await
    }

    pub async fn download(self) -> Result<PartStream<ChunkStream>, CondowError> {
        PartStream::from_chunk_stream(self.download_chunks().await?)
    }
}
