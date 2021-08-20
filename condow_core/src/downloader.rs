use crate::{
    condow_client::CondowClient,
    errors::{CondowError, GetSizeError},
    reporter::{NoReporter, Reporter, ReporterFactory},
    streams::{ChunkStream, PartStream},
    Condow, DownloadRange, GetSizeMode, StreamWithReport,
};

/// A configured downloader.
///
/// This struct has state which configures a download.
pub struct Downloader<C: CondowClient, RF: ReporterFactory = NoReporter> {
    /// Mode for handling upper bounds of a range and open ranges
    ///
    /// Default: As configured with [Condow] itself
    pub get_size_mode: GetSizeMode,
    condow: Condow<C, RF>,
}

impl<C: CondowClient, RF: ReporterFactory> Downloader<C, RF> {
    pub(crate) fn new(condow: Condow<C, RF>) -> Self {
        Self {
            condow,
            get_size_mode: GetSizeMode::default(),
        }
    }
}

impl<C: CondowClient, RF: ReporterFactory> Downloader<C, RF> {
    /// Change the behaviour on when to query the file size
    pub fn get_size_mode<T: Into<GetSizeMode>>(mut self, get_size_mode: T) -> Self {
        self.get_size_mode = get_size_mode.into();
        self
    }

    /// Download the BLOB/range.
    ///
    /// The parts and the chunks streamed have the same ordering as
    /// within the BLOB/range downloaded.
    pub async fn download<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<PartStream<ChunkStream>, CondowError> {
        self.download_chunks(location, range)
            .await
            .and_then(PartStream::from_chunk_stream)
    }

    /// Download the chunks of a BLOB/range as received
    /// from the concurrently downloaded parts.
    ///
    /// The parts and the chunks streamed have no specific ordering.
    /// Chunks of the same part still have the correct ordering as they are
    /// downloaded sequentially.
    pub async fn download_chunks<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<ChunkStream, CondowError> {
        self.condow
            .download_chunks_internal(location, range, self.get_size_mode, NoReporter)
            .await
            .map(|o| o.stream)
    }

    /// Download the BLOB/range and report events.
    ///
    /// The [Reporter] is the one that was configured when creating [Condow].
    ///
    /// The parts and the chunks streamed have the same ordering as
    /// within the BLOB/range downloaded.
    pub async fn download_rep<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<StreamWithReport<PartStream<ChunkStream>, RF::ReporterType>, CondowError> {
        let reporter = self.condow.reporter_factory.make();
        self.download_wrep(location, range, reporter).await
    }

    /// Download the chunks of a BLOB/range as received
    /// from the concurrently downloaded parts and report events.
    ///
    /// The [Reporter] is the one that was configured when creating [Condow].
    ///
    /// The parts and the chunks streamed have no specific ordering.
    /// Chunks of the same part still have the correct ordering as they are
    /// downloaded sequentially.
    pub async fn download_chunks_rep<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<StreamWithReport<ChunkStream, RF::ReporterType>, CondowError> {
        let reporter = self.condow.reporter_factory.make();
        self.download_chunks_wrep(location, range, reporter).await
    }

    /// Download the BLOB/range and report events.
    ///
    /// The [Reporter] has to be passed to the method explicitly.
    ///
    /// The parts and the chunks streamed have the same ordering as
    /// within the BLOB/range downloaded.
    pub async fn download_wrep<R: Into<DownloadRange>, RP: Reporter>(
        &self,
        location: C::Location,
        range: R,
        reporter: RP,
    ) -> Result<StreamWithReport<PartStream<ChunkStream>, RP>, CondowError> {
        self.download_chunks_wrep(location, range, reporter)
            .await?
            .part_stream()
    }

    /// Download the chunks of a BLOB/range as received
    /// from the concurrently downloaded parts and report events.
    ///
    /// The [Reporter] has to be passed to the method explicitly.
    ///
    /// The parts and the chunks streamed have no specific ordering.
    /// Chunks of the same part still have the correct ordering as they are
    /// downloaded sequentially.
    pub async fn download_chunks_wrep<R: Into<DownloadRange>, RP: Reporter>(
        &self,
        location: C::Location,
        range: R,
        reporter: RP,
    ) -> Result<StreamWithReport<ChunkStream, RP>, CondowError> {
        self.condow
            .download_chunks_internal(location, range, self.get_size_mode, reporter)
            .await
    }

    /// Get the size of a file at the BLOB at location
    pub async fn get_size(&self, location: C::Location) -> Result<usize, GetSizeError> {
        self.condow.get_size(location).await
    }
}
