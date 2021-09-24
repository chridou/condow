//! # Condow
//!
//! ## Overview
//!
//! Condow is a CONcurrent DOWnloader which downloads BLOBs
//! by splitting the download into parts and downloading them
//! concurrently.
//!
//! Some services/technologies/backends can have their download
//! speed improved, if BLOBs are downloaded concurrently by
//! "opening multiple connections". An example for this is AWS S3.
//!
//! This crate provides the core functionality only. To actually
//! use it, use one of the implementation crates:
//!
//! * `condow_rusoto`: AWS S3
//!
//! All that is required to add more "services" is to implement
//! the [CondowClient] trait.
use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt, Stream};

use condow_client::CondowClient;
use config::{AlwaysGetSize, Config};
use errors::CondowError;
use multi::MultiRangeDownloader;
use reader::RandomAccessReader;
use reporter::{NoReporting, Reporter, ReporterFactory};
use streams::{ChunkStream, ChunkStreamItem, PartStream};

#[macro_use]
pub(crate) mod helpers;
pub mod condow_client;
pub mod config;
mod download_range;
mod download_session;
mod downloader;
pub mod errors;
mod machinery;
pub mod multi;
pub mod reader;
pub mod reporter;
pub mod streams;

pub use download_range::*;
pub use download_session::*;
pub use downloader::*;

#[cfg(test)]
pub mod test_utils;

/// A common interface for downloading
pub trait Downloads<L>
where
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    /// Download a BLOB range concurrently
    ///
    /// Returns a stream of [Chunk](streams::Chunk)s.
    fn download<'a, R: Into<DownloadRange> + Send + Sync + 'static>(
        &'a self,
        location: L,
        range: R,
    ) -> BoxFuture<'a, Result<PartStream<ChunkStream>, CondowError>>;

    /// Download a BLOB range concurrently
    ///
    /// Returns a stream of [Parts](streams::Part)s.
    fn download_chunks<'a, R: Into<DownloadRange> + Send + Sync + 'static>(
        &'a self,
        location: L,
        range: R,
    ) -> BoxFuture<'a, Result<ChunkStream, CondowError>>;

    /// Get the size of a file at the BLOB location
    fn get_size<'a>(&'a self, location: L) -> BoxFuture<'a, Result<u64, CondowError>>;

    /// Creates a [RandomAccessReader] for the given location
    fn reader<'a>(
        &'a self,
        location: L,
    ) -> BoxFuture<'a, Result<RandomAccessReader<Self, L>, CondowError>>
    where
        Self: Sized + Sync,
    {
        let me = self;
        async move {
            let length = me.get_size(location.clone()).await?;
            Ok(me.reader_with_length(location, length))
        }
        .boxed()
    }

    /// Creates a [RandomAccessReader] for the given location
    fn reader_with_length(&self, location: L, length: u64) -> RandomAccessReader<Self, L>
    where
        Self: Sized;
}

/// The CONcurrent DOWnloader
///
/// Downloads BLOBs by splitting the download into parts
/// which are downloaded concurrently.
///
/// The API of `Condow` itself should be sufficient for most use cases.
///
/// If reporting/metrics is required, see [Downloader] and [DownloadSession]
///
/// ## Wording
///
/// * `Range`: A range to be downloaded of a BLOB (Can also be the complete BLOB)
/// * `Part`: The downloaded range is split into parts of certain ranges which are downloaded concurrently
/// * `Chunk`: A chunk of bytes received from the network (or else). Multiple chunks make a part.
pub struct Condow<C> {
    client: C,
    config: Config,
}

impl<C: CondowClient> Clone for Condow<C> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
        }
    }
}

impl<C: CondowClient> Condow<C> {
    /// Create a new CONcurrent DOWnloader.
    ///
    /// Fails if the [Config] is not valid.
    pub fn new(client: C, config: Config) -> Result<Self, anyhow::Error> {
        let config = config.validated()?;
        Ok(Self { client, config })
    }

    /// Create a reusable [Downloader] which has a richer API.
    pub fn downloader(&self) -> Downloader<C, NoReporting> {
        Downloader::new(self.clone())
    }

    /// Create a reusable [Downloader] which has a richer API.
    pub fn downloader_with_reporting<RF: ReporterFactory>(&self, rep_fac: RF) -> Downloader<C, RF> {
        self.downloader_with_reporting_arc(Arc::new(rep_fac))
    }

    /// Create a reusable [Downloader] which has a richer API.
    pub fn downloader_with_reporting_arc<RF: ReporterFactory>(
        &self,
        rep_fac: Arc<RF>,
    ) -> Downloader<C, RF> {
        Downloader::new_with_reporting_arc(self.clone(), rep_fac)
    }

    /// Create a [DownloadSession] to track all downloads.
    pub fn download_session<RF: ReporterFactory>(&self, rep_fac: RF) -> DownloadSession<C, RF> {
        self.download_session_arc(Arc::new(rep_fac))
    }

    /// Create a [DownloadSession] to track all downloads.
    pub fn download_session_arc<RF: ReporterFactory>(
        &self,
        rep_fac: Arc<RF>,
    ) -> DownloadSession<C, RF> {
        DownloadSession::new_with_reporting_arc(self.clone(), rep_fac)
    }

    /// Create a new [MultiRangeDownloader] without reporting
    pub fn multi_range_downloader(&self) -> MultiRangeDownloader<C> {
        MultiRangeDownloader::new_with_reporting_arc(self.clone(), Arc::new(NoReporting))
    }

    /// Download a BLOB range (potentially) concurrently
    ///
    /// Returns a stream of [Chunk](streams::Chunk)s.
    pub async fn download_chunks<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<ChunkStream, CondowError> {
        machinery::download(self, location, range, GetSizeMode::Default, NoReporting)
            .await
            .map(|o| o.into_stream())
    }

    /// Download a BLOB range (potentially) concurrently
    ///
    /// Returns a stream of [Parts](streams::Part)s.
    pub async fn download<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<PartStream<ChunkStream>, CondowError> {
        let chunk_stream =
            machinery::download(self, location, range, GetSizeMode::Default, NoReporting)
                .await
                .map(|o| o.into_stream())?;
        PartStream::from_chunk_stream(chunk_stream)
    }

    /// Get the size of a file at the given location
    pub async fn get_size(&self, location: C::Location) -> Result<u64, CondowError> {
        self.client.get_size(location).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub async fn reader(
        &self,
        location: C::Location,
    ) -> Result<RandomAccessReader<Self, C::Location>, CondowError> {
        RandomAccessReader::new(self.clone(), location).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub fn reader_with_length(
        &self,
        location: C::Location,
        length: u64,
    ) -> RandomAccessReader<Self, C::Location> {
        RandomAccessReader::new_with_length(self.clone(), location, length)
    }
}

impl<C> Downloads<C::Location> for Condow<C>
where
    C: CondowClient,
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

    fn get_size<'a>(&'a self, location: C::Location) -> BoxFuture<'a, Result<u64, CondowError>> {
        Box::pin(self.get_size(location))
    }

    fn reader_with_length(
        &self,
        location: C::Location,
        length: u64,
    ) -> RandomAccessReader<Self, C::Location> {
        Condow::reader_with_length(self, location, length)
    }
}

/// A composite struct of a stream and a [Reporter]
///
/// Returned from functions which have reporting enabled.
pub struct StreamWithReport<St: Stream, R: Reporter> {
    pub stream: St,
    pub reporter: R,
}

impl<St, R> StreamWithReport<St, R>
where
    St: Stream,
    R: Reporter,
{
    pub fn new(stream: St, reporter: R) -> Self {
        Self { stream, reporter }
    }

    pub fn into_stream(self) -> St {
        self.stream
    }

    pub fn into_parts(self) -> (St, R) {
        (self.stream, self.reporter)
    }
}

impl<R> StreamWithReport<ChunkStream, R>
where
    R: Reporter,
{
    pub fn part_stream(self) -> Result<StreamWithReport<PartStream<ChunkStream>, R>, CondowError> {
        let StreamWithReport { stream, reporter } = self;
        let part_stream = PartStream::from_chunk_stream(stream)?;
        Ok(StreamWithReport::new(part_stream, reporter))
    }

    pub async fn write_buffer(self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        self.stream.write_buffer(buffer).await
    }

    pub async fn into_vec(self) -> Result<Vec<u8>, CondowError> {
        self.stream.into_vec().await
    }
}

impl<S, R> StreamWithReport<PartStream<S>, R>
where
    S: Stream<Item = ChunkStreamItem> + Send + Sync + 'static + Unpin,
    R: Reporter,
{
    pub async fn write_buffer(self, buffer: &mut [u8]) -> Result<usize, CondowError> {
        self.stream.write_buffer(buffer).await
    }

    pub async fn into_vec(self) -> Result<Vec<u8>, CondowError> {
        self.stream.into_vec().await
    }
}

/// Overide the behaviour when [Condow] does a request to get
/// the size of a BLOB
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GetSizeMode {
    /// Request a size on open ranges and also closed ranges
    /// so that a given upper bound can be adjusted/corrected
    Always,
    /// Only request the size of a BLOB when required. This is when an open
    /// range (e.g. complete BLOB or from x to end)
    Required,
    /// As configured with [Condow] itself.
    Default,
}

impl GetSizeMode {
    fn is_load_size_enforced(self, always_by_default: AlwaysGetSize) -> bool {
        match self {
            GetSizeMode::Always => true,
            GetSizeMode::Required => false,
            GetSizeMode::Default => always_by_default.into_inner(),
        }
    }
}

impl Default for GetSizeMode {
    fn default() -> Self {
        Self::Default
    }
}

#[cfg(test)]
mod condow_tests;
