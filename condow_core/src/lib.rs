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
//! ## Usage
//!
//! To use condow a client to access remote data is required. In the examples below
//! [InMemoryClient] is used. Usually this would be some client which really accesses
//! remote BLOBs.
//!
//! The [Condow] struct itself can be used to download BLOBs. It might not be
//! convinient to pass it around since it has 2 type parameters. Consider the
//! traits [Downloads] (which has only an associated type) or [DownloadUntyped]
//! (which is even object safe) to pass around instances of [Condow].
//!
//! ```
//! use condow_core::condow_client::InMemoryClient;
//! use condow_core::{Condow, config::Config};
//!
//! # #[tokio::main]
//! # async fn main() {
//! // First we need a client...
//! let client = InMemoryClient::<String>::new_static(b"a remote BLOB");
//!
//! // ... and a configuration for Condow
//! let config = Config::default();
//!
//! let condow = Condow::new(client, config).unwrap();
//!
//! assert_eq!(condow.get_size("a location").await.unwrap(), 13);
//!
//! // Download the complete BLOB
//! let blob = condow.blob().at("a location").download_into_vec().await.unwrap();
//! assert_eq!(blob, b"a remote BLOB");
//!
//! // Download part of a BLOB. Any Rust range syntax will work.
//! let blob = condow.blob().at("a location").range(2..=7).download_into_vec().await.unwrap();
//! assert_eq!(blob, b"remote");
//!
//! let blob = condow.blob().at("a location").range(2..).download_into_vec().await.unwrap();
//! assert_eq!(blob, b"remote BLOB");
//! # }
//! ```
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
//! ## Instrumentation
//!
//! Instrumentation can be done for each individual download or centralized
//! for global monitoring. For further information see the [probe] module.
//!
//! [condow_rusoto]:https://docs.rs/condow_rusoto
//! [condow_fs]:https://docs.rs/condow_fs
//! [InMemoryClient]:condow_client::InMemoryClient
use std::{str::FromStr, sync::Arc};

use anyhow::Error as AnyError;
use futures::{future::BoxFuture, FutureExt};

use condow_client::CondowClient;
use config::{ClientRetryWrapper, Config};
use errors::CondowError;
use machinery::ProbeInternal;
use probe::{Probe, ProbeFactory};
use reader::{CondowAdapter, RandomAccessReader};
use streams::{ChunkStream, OrderedChunkStream};

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
///
/// This trait is not object safe. If you want to use dynamic dispatch you
/// might consider the trait [DownloadsUntyped] which is object safe
/// and accepts string slices as a location.
pub trait Downloads: Send + Sync + 'static {
    type Location: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static;

    /// Download a BLOB via the returned request object
    fn blob(&self) -> RequestNoLocation<Self::Location>;

    /// Get the size of a BLOB at the given location
    fn get_size<'a>(&'a self, location: Self::Location) -> BoxFuture<'a, Result<u64, CondowError>>;

    /// Creates a [RandomAccessReader] for the given location
    ///
    /// This function will query the size of the BLOB. If the size is already known
    /// call [Downloads::reader_with_length]
    fn reader<'a>(
        &'a self,
        location: Self::Location,
    ) -> BoxFuture<'a, Result<RandomAccessReader, CondowError>>
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
    fn reader_with_length(&self, location: Self::Location, length: u64) -> RandomAccessReader
    where
        Self: Sized;
}

/// Downloads from a location specified by a &[str].
///
/// This trait is object safe
///
///  ```
/// # use std::sync::Arc;
/// use condow_core::{Condow, DownloadsUntyped, config::Config};
/// use condow_core::condow_client::InMemoryClient;
///
/// # #[tokio::main]
/// # async fn main() {
/// // First we need a client... Let's make the type of the location simply `u32`s
/// let client = InMemoryClient::<u32>::new_static(b"a remote BLOB");
///
/// // ... and a configuration for Condow
/// let config = Config::default();
///
/// let condow = Condow::new(client, config).unwrap();
///
/// // The trait is object save:
/// let downloader: Arc<dyn DownloadsUntyped> = Arc::new(condow);
///
/// // "42" parses as `u32`
/// assert_eq!(downloader.get_size("42").await.unwrap(), 13);
///
/// // "x" does not
/// assert!(downloader.get_size("x").await.is_err());
///
/// let blob = downloader.blob().at("42").download_into_vec().await.unwrap();
/// assert_eq!(blob, b"a remote BLOB");
/// # }
/// ```
pub trait DownloadsUntyped: Send + Sync + 'static {
    /// Download a BLOB via the returned request object
    fn blob(&self) -> RequestNoLocation<&str>;

    /// Get the size of a BLOB at the given location
    ///
    /// A location which can not be parsed causes method to fail.
    fn get_size<'a>(&'a self, location: &str) -> BoxFuture<'a, Result<u64, CondowError>>;

    /// Creates a [RandomAccessReader] for the given location
    ///
    /// This function will query the size of the BLOB. If the size is already known
    /// call [Downloads::reader_with_length]
    ///
    /// A location which can not be parsed causes method to fail.
    fn reader<'a>(
        &'a self,
        location: &'a str,
    ) -> BoxFuture<'a, Result<RandomAccessReader, CondowError>>
    where
        Self: Sized + Sync,
    {
        async move {
            let length = self.get_size(location).await?;
            self.reader_with_length(location, length)
        }
        .boxed()
    }

    /// Creates a [RandomAccessReader] for the given location
    ///
    /// This function will create a new reader immediately
    ///
    /// A location which can not be parsed causes method to fail.
    fn reader_with_length(
        &self,
        location: &str,
        length: u64,
    ) -> Result<RandomAccessReader, CondowError>
    where
        Self: Sized;
}

/// The CONcurrent DOWnloader
///
/// Downloads BLOBs by splitting the download into parts
/// which are downloaded concurrently.
///
/// It is recommended to use one of the traits [Downloads] or
/// [DownloadsUntyped] instead of [Condow] itself since this
/// struct might get more type parameters in the future.
///
/// ## Wording
///
/// * `Range`: A range to be downloaded of a BLOB (Can also be the complete BLOB)
/// * `Part`: The downloaded range is split into parts of certain ranges which are downloaded concurrently
/// * `Chunk`: A chunk of bytes received from the network (or else). Multiple chunks make up a part.
pub struct Condow<C, PF = ()> {
    client: ClientRetryWrapper<C>,
    config: Config,
    probe_factory: Option<Arc<PF>>,
}

impl<C: CondowClient, PF: ProbeFactory> Clone for Condow<C, PF> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            probe_factory: self.probe_factory.clone(),
        }
    }
}

impl<C> Condow<C>
where
    C: CondowClient,
{
    /// Create a new CONcurrent DOWnloader.
    ///
    /// Fails if the [Config] is not valid.
    pub fn new(client: C, config: Config) -> Result<Condow<C, ()>, AnyError> {
        let config = config.validated()?;
        Ok(Condow {
            client: ClientRetryWrapper::new(client, config.retries.clone()),
            config,
            probe_factory: None,
        })
    }
}

impl<C, PF> Condow<C, PF>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    /// Set a factory for [Probe]s which will add a [Probe] to each request
    ///
    /// The [ProbeFactory] is intended to share state with the [Probe] to
    /// add instrumentation
    pub fn probe_factory<PPF: ProbeFactory>(self, factory: PPF) -> Condow<C, PPF> {
        self.probe_factory_shared(Arc::new(factory))
    }

    /// Set a factory for [Probe]s which will add a [Probe] to each request
    ///
    /// The [ProbeFactory] is intended to share state with the [Probe] to
    /// add instrumentation
    pub fn probe_factory_shared<PPF: ProbeFactory>(self, factory: Arc<PPF>) -> Condow<C, PPF> {
        Condow {
            client: self.client,
            config: self.config,
            probe_factory: Some(factory),
        }
    }

    /// Download a BLOB via the returned request object
    pub fn blob(&self) -> RequestNoLocation<C::Location> {
        let condow = self.clone();
        let download_fn = move |location: <C as CondowClient>::Location, params: Params| {
            let probe = match (
                params.probe,
                condow.probe_factory.as_ref().map(|f| f.make(&location)),
            ) {
                (None, None) => ProbeInternal::Off,
                (Some(req), None) => ProbeInternal::OneDyn(req),
                (None, Some(fac)) => ProbeInternal::OneStatic(fac),
                (Some(req), Some(fac)) => ProbeInternal::Two(fac, req),
            };

            machinery::download_range(
                condow.client,
                condow.config,
                location,
                params.range,
                probe,
            )
            .boxed()
        };

        RequestNoLocation::new(download_fn, self.config.clone())
    }

    /// Get the size of a BLOB at the given location
    pub async fn get_size<L: Into<C::Location>>(&self, location: L) -> Result<u64, CondowError> {
        self.client
            .get_size(location.into(), &Default::default())
            .await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub async fn reader<L: Into<C::Location>>(
        &self,
        location: L,
    ) -> Result<RandomAccessReader, CondowError> {
        RandomAccessReader::new(CondowAdapter::new(self.clone(), location.into())).await
    }

    /// Creates a [RandomAccessReader] for the given location
    pub fn reader_with_length<L: Into<C::Location>>(
        &self,
        location: L,
        length: u64,
    ) -> RandomAccessReader {
        RandomAccessReader::new_with_length(
            CondowAdapter::new(self.clone(), location.into()),
            length,
        )
    }
}

impl<C, PF> Downloads for Condow<C, PF>
where
    C: CondowClient,
    PF: ProbeFactory,
{
    type Location = C::Location;

    fn blob(&self) -> RequestNoLocation<Self::Location> {
        self.blob()
    }

    fn get_size<'a>(&'a self, location: Self::Location) -> BoxFuture<'a, Result<u64, CondowError>> {
        Box::pin(self.get_size(location))
    }

    fn reader_with_length(&self, location: Self::Location, length: u64) -> RandomAccessReader
    where
        Self: Sized,
    {
        self.reader_with_length(location, length)
    }
}

impl<C, PF> DownloadsUntyped for Condow<C, PF>
where
    C: CondowClient,
    C::Location: FromStr,
    <C::Location as FromStr>::Err: std::error::Error + Sync + Send + 'static,
    PF: ProbeFactory,
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
                (Some(req), None) => ProbeInternal::OneDyn(req),
                (None, Some(fac)) => ProbeInternal::OneStatic(fac),
                (Some(req), Some(fac)) => ProbeInternal::Two(fac, req),
            };

            machinery::download_range(
                condow.client,
                condow.config,
                location,
                params.range,
                probe,
            )
            .boxed()
        };

        RequestNoLocation::new(download_fn, self.config.clone())
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

    fn reader_with_length(
        &self,
        location: &str,
        length: u64,
    ) -> Result<RandomAccessReader, CondowError>
    where
        Self: Sized,
    {
        let location = match location.parse::<C::Location>() {
            Ok(loc) => loc,
            Err(parse_err) => {
                return Err(
                    CondowError::new_other(format!("invalid location: {location}"))
                        .with_source(parse_err),
                )
            }
        };

        Ok(self.reader_with_length(location, length))
    }
}

#[cfg(test)]
mod condow_tests;

#[test]
fn downloads_untyped_is_object_safe_must_compile() {
    struct Foo;

    impl DownloadsUntyped for Foo {
        fn blob(&self) -> RequestNoLocation<&str> {
            todo!()
        }

        fn get_size<'a>(&'a self, _location: &str) -> BoxFuture<'a, Result<u64, CondowError>> {
            todo!()
        }

        fn reader_with_length(
            &self,
            _location: &str,
            _length: u64,
        ) -> Result<RandomAccessReader, CondowError>
        where
            Self: Sized,
        {
            todo!()
        }
    }

    let _: Box<dyn DownloadsUntyped> = Box::new(Foo);
}
