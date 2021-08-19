use crate::{
    condow_client::CondowClient,
    errors::{CondowError, GetSizeError},
    reporter::{NoReporter, ReporterFactory},
    streams::{ChunkStream, PartStream},
    Condow, DownloadRange, GetSizeMode, Outcome,
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

    /// Download the chunks of a BLOB/range as received
    /// from the concurrently downloaded parts.
    ///
    /// The parts and the chunks streamed have no specific ordering.
    /// Chunks of the same part still have the correct ordering as they are
    /// downloaded sequentially.
    ///
    /// See also [download](Condow::download)
    pub async fn download_chunks<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<Outcome<ChunkStream, RF::ReporterType>, CondowError> {
        self.condow
            .download_chunks_internal(location, range, self.get_size_mode)
            .await
    }

    /// Download the BLOB/range.
    ///
    /// The parts and the chunks streamed have the same ordering as
    /// within the BLOB/range downloaded.
    ///
    /// See also [download](Condow::download)
    pub async fn download<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<Outcome<PartStream<ChunkStream>, RF::ReporterType>, CondowError> {
        self.download_chunks(location, range).await?.part_stream()
    }

    /// Get the size of a file at the BLOB at location
    pub async fn get_size(&self, location: C::Location) -> Result<usize, GetSizeError> {
        self.condow.get_size(location).await
    }
}
