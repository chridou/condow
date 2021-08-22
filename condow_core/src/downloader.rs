/// Downloading API with optional per request instrumentation
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::{
    condow_client::CondowClient,
    errors::CondowError,
    machinery,
    reporter::{NoReporting, Reporter, ReporterFactory},
    streams::{ChunkStream, PartStream},
    Condow, DownloadRange, Downloads, GetSizeMode, StreamWithReport,
};

/// A downloading API.
///
/// This has mutiple methods to download data. The main difference to
/// [Condow] itself is, that per request reporting/instrumentation can be enabled.
/// Only those methods which return a [Reporter] will be instrumented.
pub struct Downloader<C: CondowClient, RF: ReporterFactory = NoReporting> {
    /// Mode for handling upper bounds of a range and open ranges
    ///
    /// Default: As configured with [Condow] itself
    /// or the struct this was cloned from
    pub get_size_mode: GetSizeMode,
    condow: Condow<C>,
    reporter_factory: Arc<RF>,
}

impl<C: CondowClient> Downloader<C, NoReporting> {
    pub(crate) fn new(condow: Condow<C>) -> Self {
        Self::new_with_reporting(condow, NoReporting)
    }
}

impl<C: CondowClient, RF: ReporterFactory> Downloader<C, RF> {
    pub(crate) fn new_with_reporting(condow: Condow<C>, rep_fac: RF) -> Self {
        Self::new_with_reporting_arc(condow, Arc::new(rep_fac))
    }

    pub(crate) fn new_with_reporting_arc(condow: Condow<C>, rep_fac: Arc<RF>) -> Self {
        Self {
            condow,
            get_size_mode: GetSizeMode::default(),
            reporter_factory: rep_fac,
        }
    }

    /// Change the behaviour on when to query the file size
    pub fn get_size_mode<T: Into<GetSizeMode>>(mut self, get_size_mode: T) -> Self {
        self.get_size_mode = get_size_mode.into();
        self
    }

    /// Set or replace the [ReporterFactory] in a builder style
    pub fn with_reporting<RRF: ReporterFactory>(self, rep_fac: RRF) -> Downloader<C, RRF> {
        self.with_reporting_arc(Arc::new(rep_fac))
    }

    /// Set or replace the [ReporterFactory] in a builder style
    pub fn with_reporting_arc<RRF: ReporterFactory>(self, rep_fac: Arc<RRF>) -> Downloader<C, RRF> {
        let Downloader {
            get_size_mode,
            condow,
            ..
        } = self;

        Downloader {
            condow,
            get_size_mode,
            reporter_factory: rep_fac,
        }
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
        machinery::start_download(
            &self.condow,
            location,
            range,
            self.get_size_mode,
            NoReporting,
        )
        .await
        .map(|o| o.stream)
    }

    /// Download the BLOB/range and report events.
    ///
    /// The returned [Reporter] is created by the [ReporterFactory] when constructed.
    ///
    /// The parts and the chunks streamed have the same ordering as
    /// within the BLOB/range downloaded.
    pub async fn download_rep<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<StreamWithReport<PartStream<ChunkStream>, RF::ReporterType>, CondowError> {
        let reporter = self.reporter_factory.make();
        self.download_wrep(location, range, reporter).await
    }

    /// Download the chunks of a BLOB/range as received
    /// from the concurrently downloaded parts and report events.
    ///
    /// The returned [Reporter] is created by the [ReporterFactory] when constructed.
    ///
    /// The parts and the chunks streamed have no specific ordering.
    /// Chunks of the same part still have the correct ordering as they are
    /// downloaded sequentially.
    pub async fn download_chunks_rep<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<StreamWithReport<ChunkStream, RF::ReporterType>, CondowError> {
        let reporter = self.reporter_factory.make();
        self.download_chunks_wrep(location, range, reporter).await
    }

    /// Download the BLOB/range and report events.
    ///
    /// A [Reporter] has to be passed to the method explicitly.
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
    /// A [Reporter] has to be passed to the method explicitly.
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
        machinery::start_download(&self.condow, location, range, self.get_size_mode, reporter).await
    }

    /// Get the size of a BLOB at location
    pub async fn get_size(&self, location: C::Location) -> Result<usize, CondowError> {
        self.condow.get_size(location).await
    }
}

impl<C: CondowClient, RF: ReporterFactory> Clone for Downloader<C, RF> {
    fn clone(&self) -> Self {
        Self {
            condow: self.condow.clone(),
            reporter_factory: Arc::clone(&self.reporter_factory),
            get_size_mode: self.get_size_mode,
        }
    }
}

impl<C, RF> Downloads<C::Location> for Downloader<C, RF>
where
    C: CondowClient,
    RF: ReporterFactory,
{
    fn download<'a, R: Into<DownloadRange> + Send + Sync + 'static>(
        &'a self,
        location: C::Location,
        range: R,
    ) -> BoxFuture<'a, Result<PartStream<ChunkStream>, CondowError>> {
        Box::pin(self.download(location, range))
    }

    fn download_chunks<'a, R: Into<DownloadRange> + Send + Sync + 'static>(
        &'a self,
        location: C::Location,
        range: R,
    ) -> BoxFuture<'a, Result<ChunkStream, CondowError>> {
        Box::pin(self.download_chunks(location, range))
    }
}
