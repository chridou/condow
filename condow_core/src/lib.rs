//! # ConDow
//!
//! ## Overview
//!
//! ConDow is a CONcurrent DOWnloader which downloads BLOBs
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
//! * [condow_rusoto] for downloading AWS S3 via the rusoto
//! * [condow_fs] for using async file access via [tokio]
//!
//! All that is required to add more "services" is to implement
//! the [CondowClient] trait.
//!
//! ## Retries
//!
//! ConDow supports retries. These can be done on the downloads themselves
//! as well on the byte streams returned from a client. If an error occurs
//! while streaming bytes ConDow will try to reconnect with retries and
//! resume streaming where the previous stream failed.
//!
//! Retries can also be attempted on size requests.
//!
//! Be aware that some clients might also do retries themselves based on
//! their underlying implementation. In this case you should disable retries for either the
//! client or ConDow itself.
//!
//! [condow_rusoto]:https://docs.rs/condow_rusoto
//! [condow_fs]:https://docs.rs/condow_fs
use futures::Stream;

use condow_client::CondowClient;
use config::{AlwaysGetSize, ClientRetryWrapper, Config};
use errors::CondowError;
use probe::Probe;
use reader::RandomAccessReader;
use streams::{ChunkStream, ChunkStreamItem, PartStream};

#[macro_use]
pub(crate) mod helpers;
pub mod condow_client;
pub mod config;
mod download_range;
pub mod errors;
mod machinery;
pub mod probe;
pub mod reader;
mod request;
mod retry;
pub mod streams;

pub use download_range::*;
pub use request::*;

#[cfg(test)]
pub mod test_utils;

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
    client: ClientRetryWrapper<C>,
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

impl<C: CondowClient> Condow<C>
where
    C: CondowClient,
{
    /// Create a new CONcurrent DOWnloader.
    ///
    /// Fails if the [Config] is not valid.
    pub fn new(client: C, config: Config) -> Result<Self, anyhow::Error> {
        let config = config.validated()?;
        Ok(Self {
            client: ClientRetryWrapper::new(client, config.retries.clone()),
            config,
        })
    }

    /// Download a Blob via the returned request object
    pub fn blob(&self) -> RequestNoLocation<C> {
        RequestNoLocation::new(self.clone())
    }

    /// Get the size of a file at the given location
    pub async fn get_size(&self, location: C::Location) -> Result<u64, CondowError> {
        self.client.get_size(location, &Default::default()).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub async fn reader(
        &self,
        location: C::Location,
    ) -> Result<RandomAccessReader<C>, CondowError> {
        RandomAccessReader::new(self.clone(), location).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub fn reader_with_length(&self, location: C::Location, length: u64) -> RandomAccessReader<C> {
        RandomAccessReader::new_with_length(self.clone(), location, length)
    }
}

/// A composite struct of a stream and a [Reporter]
///
/// Returned from functions which have reporting enabled.
pub struct StreamWithReport<St: Stream, R: Probe> {
    pub stream: St,
    pub reporter: R,
}

impl<St, R> StreamWithReport<St, R>
where
    St: Stream,
    R: Probe,
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
    R: Probe,
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
    R: Probe,
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
