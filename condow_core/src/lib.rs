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
use std::{str::FromStr, sync::Arc};

use futures::{future::BoxFuture, FutureExt};

use condow_client::CondowClient;
use config::{AlwaysGetSize, ClientRetryWrapper, Config};
use errors::CondowError;
use machinery::ProbeInternal;
use probe::{Probe, ProbeFactory};
use reader::RandomAccessReader;
use streams::{ChunkStream, PartStream};

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

/// A common interface for downloading
pub trait Downloads {
    type Location: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static;

    /// Download a Blob via the returned request object
    fn blob(&self) -> RequestNoLocation<Self::Location>;

    /// Get the size of a file at the BLOB location
    fn get_size<'a>(&'a self, location: Self::Location) -> BoxFuture<'a, Result<u64, CondowError>>;

    /// Creates a [RandomAccessReader] for the given location
    ///
    /// This function will query the size of the BLOB. If the size is already known
    /// call [Downloads::reader_with_length]
    fn reader<'a>(
        &'a self,
        location: Self::Location,
    ) -> BoxFuture<'a, Result<RandomAccessReader<Self>, CondowError>>
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
    ///
    /// This function will create a new reader immediately
    fn reader_with_length(&self, location: Self::Location, length: u64) -> RandomAccessReader<Self>
    where
        Self: Sized;
}

pub trait DownloadsUntyped {
    /// Download a Blob via the returned request object
    fn blob(&self) -> RequestNoLocation<&str>;
    /// Get the size of a file at the BLOB location
    fn get_size<'a>(&'a self, location: &str) -> BoxFuture<'a, Result<u64, CondowError>>;
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
    client: ClientRetryWrapper<C>,
    config: Config,
    probe_factory: Option<Arc<dyn ProbeFactory>>,
}

impl<C: CondowClient> Clone for Condow<C> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            probe_factory: self.probe_factory.clone(),
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
            probe_factory: None,
        })
    }

    /// Set a factory for [Probe]s which will add a [Probe] to each request
    ///
    /// The [ProbeFactory] is intended to share state with the [Probe] to
    /// add instrumentation
    pub fn set_probe_factory(&mut self, factory: Arc<dyn ProbeFactory>) {
        self.probe_factory = Some(factory);
    }

    /// Download a Blob via the returned request object
    pub fn blob(&self) -> RequestNoLocation<C::Location> {
        let condow = self.clone();
        let download_fn = move |location: <C as CondowClient>::Location, params: Params| {
            let probe = match (
                params.probe,
                condow.probe_factory.as_ref().map(|f| f.make(&location)),
            ) {
                (None, None) => ProbeInternal::Off,
                (Some(req), None) => ProbeInternal::One(req),
                (None, Some(fac)) => ProbeInternal::One(fac),
                (Some(req), Some(fac)) => ProbeInternal::Two(req, fac),
            };

            machinery::download_range(condow, location, params.range, params.get_size_mode, probe)
                .boxed()
        };

        RequestNoLocation::new(download_fn)
    }

    /// Get the size of a file at the given location
    pub async fn get_size(&self, location: C::Location) -> Result<u64, CondowError> {
        self.client.get_size(location, &Default::default()).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub async fn reader(
        &self,
        location: C::Location,
    ) -> Result<RandomAccessReader<Self>, CondowError> {
        RandomAccessReader::new(self.clone(), location).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub fn reader_with_length(
        &self,
        location: C::Location,
        length: u64,
    ) -> RandomAccessReader<Self> {
        RandomAccessReader::new_with_length(self.clone(), location, length)
    }
}

impl<C> Downloads for Condow<C>
where
    C: CondowClient,
{
    type Location = C::Location;

    fn blob(&self) -> RequestNoLocation<Self::Location> {
        self.blob()
    }

    fn get_size<'a>(&'a self, location: Self::Location) -> BoxFuture<'a, Result<u64, CondowError>> {
        Box::pin(self.get_size(location))
    }

    fn reader_with_length(&self, location: Self::Location, length: u64) -> RandomAccessReader<Self>
    where
        Self: Sized,
    {
        self.reader_with_length(location, length)
    }
}

impl<C> DownloadsUntyped for Condow<C>
where
    C: CondowClient,
    C::Location: FromStr,
    <C::Location as FromStr>::Err: std::error::Error + Sync + Send + 'static,
{
    fn blob(&self) -> RequestNoLocation<&str> {
        let condow = self.clone();
        let download_fn = move |location: &str, params: Params| {
            let location = match location.parse::<C::Location>() {
                Ok(loc) => loc,
                Err(parse_err) => {
                    return futures::future::err(
                        CondowError::new_other(format!("invalid location: {location}"))
                            .with_source(parse_err),
                    )
                    .boxed();
                }
            };

            let probe = match (
                params.probe,
                condow.probe_factory.as_ref().map(|f| f.make(&location)),
            ) {
                (None, None) => ProbeInternal::Off,
                (Some(req), None) => ProbeInternal::One(req),
                (None, Some(fac)) => ProbeInternal::One(fac),
                (Some(req), Some(fac)) => ProbeInternal::Two(req, fac),
            };

            machinery::download_range(condow, location, params.range, params.get_size_mode, probe)
                .boxed()
        };

        RequestNoLocation::new(download_fn)
    }

    fn get_size<'a>(&'a self, location: &str) -> BoxFuture<'a, Result<u64, CondowError>> {
        let location = match location.parse::<C::Location>() {
            Ok(loc) => loc,
            Err(parse_err) => {
                return futures::future::err(
                    CondowError::new_other(format!("invalid location: {location}"))
                        .with_source(parse_err),
                )
                .boxed();
            }
        };

        Box::pin(self.get_size(location))
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
