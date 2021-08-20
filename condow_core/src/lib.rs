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

use condow_client::CondowClient;
use config::{AlwaysGetSize, Config};
use errors::{CondowError, GetSizeError};

use futures::Stream;
use reporter::{NoReporting, Reporter, ReporterFactory};
use streams::{BytesHint, ChunkStream, ChunkStreamItem, PartStream};

#[macro_use]
pub(crate) mod helpers;
pub mod condow_client;
pub mod config;
mod download_range;
mod downloader;
pub mod errors;
mod machinery;
pub mod reporter;
pub mod streams;

pub use download_range::*;
pub use downloader::*;

#[cfg(test)]
pub mod test_utils;

/// The CONcurrent DOWnloader
///
/// Downloads BLOBs by splitting the download into parts
/// which are downloaded concurrently.
///
/// ## Wording
///
/// * `Range`: A range to be downloaded of a BLOB (Can also be the complete BLOB)
/// * `Part`: The downloaded range is split into parts of certain ranges which are downloaded concurrently
/// * `Chunk`: A chunk of bytes received from the network (or else). Multiple chunks make a part.
pub struct Condow<C, RF = NoReporting> {
    client: C,
    config: Config,
    reporter_factory: Arc<RF>,
}

impl<C: CondowClient, RF: ReporterFactory> Clone for Condow<C, RF> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            reporter_factory: Arc::clone(&self.reporter_factory),
        }
    }
}

impl<C: CondowClient> Condow<C, NoReporting> {
    /// Create a new CONcurrent DOWnloader.
    ///
    /// Fails if the [Config] is not valid.
    pub fn new(client: C, config: Config) -> Result<Self, anyhow::Error> {
        let config = config.validated()?;
        Self::new_with_reporting(client, config, NoReporting)
    }
}

impl<C: CondowClient, RF: ReporterFactory> Condow<C, RF> {
    pub fn new_with_reporting(
        client: C,
        config: Config,
        rep_fac: RF,
    ) -> Result<Self, anyhow::Error> {
        let config = config.validated()?;
        Ok(Self {
            client,
            config,
            reporter_factory: Arc::new(rep_fac),
        })
    }

    /// Create a reusable [Downloader] which is just an alternate form to use the API.
    pub fn downloader(&self) -> Downloader<C, RF> {
        Downloader::new(self.clone())
    }

    /// Download a BLOB range (potentially) concurrently
    ///
    /// Returns a stream of [Chunk](streams::Chunk)s.
    pub async fn download_chunks<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<ChunkStream, CondowError> {
        self.download_chunks_internal(location, range, GetSizeMode::Default, NoReporting)
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
        let chunk_stream = self
            .download_chunks_internal(location, range, GetSizeMode::Default, NoReporting)
            .await
            .map(|o| o.into_stream())?;
        PartStream::from_chunk_stream(chunk_stream)
    }

    /// Get the size of a file at the given location
    pub async fn get_size(&self, location: C::Location) -> Result<usize, GetSizeError> {
        self.client.get_size(location).await
    }

    async fn download_chunks_internal<R: Into<DownloadRange>, RP: Reporter>(
        &self,
        location: C::Location,
        range: R,
        get_size_mode: GetSizeMode,
        reporter: RP,
    ) -> Result<StreamWithReport<ChunkStream, RP>, CondowError> {
        let range: DownloadRange = range.into();
        range.validate()?;
        let range = if let Some(range) = range.sanitized() {
            range
        } else {
            return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
        };

        let (inclusive_range, bytes_hint) = match range {
            DownloadRange::Open(or) => {
                let size = self.client.get_size(location.clone()).await?;
                if let Some(range) = or.incl_range_from_size(size) {
                    (range, BytesHint::new_exact(range.len()))
                } else {
                    return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
                }
            }
            DownloadRange::Closed(cl) => {
                if get_size_mode.is_load_size_enforced(self.config.always_get_size) {
                    let size = self.client.get_size(location.clone()).await?;
                    if let Some(range) = cl.incl_range_from_size(size) {
                        (range, BytesHint::new_exact(range.len()))
                    } else {
                        return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
                    }
                } else if let Some(range) = cl.incl_range() {
                    (range, BytesHint::new_at_max(range.len()))
                } else {
                    return Ok(StreamWithReport::new(ChunkStream::empty(), reporter));
                }
            }
        };

        let stream = machinery::download(
            self.client.clone(),
            location,
            inclusive_range,
            bytes_hint,
            self.config.clone(),
            reporter.clone(),
        )
        .await?;

        Ok(StreamWithReport { reporter, stream })
    }
}

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
    S: Stream<Item = ChunkStreamItem> + Unpin,
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
