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
//! convenient to pass it around since it has 2 type parameters. Consider the
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
//! ## Behaviour
//!
//! Downloads with a maximum concurrency of 3 are streamed on the same task the download
//! was initiated on. This means that the returned stream needs to be polled to drive
//! pulling chunks from the network. Executing the streaming also means that panics
//! of underlying libraries will pop up on the polling task.
//!
//! Downloads with a concurrency greater or equal than 4 are executed on dedicated tasks.
//! Panics will be detected and the stream will abort with an error.
//!
//! With the [EnsureActivePull] config setting all downloads will be executed on dedicated tasks and
//! panics will be detected.
//!
//! All downloads executed on dedicated tasks will pull bytes from the network eagerly
//! and fill a queue.
//!
//! ## Instrumentation
//!
//! Instrumentation can be done for each individual download or centralized
//! for global monitoring. For further information see the [probe] module.
//!
//! [condow_rusoto]:https://docs.rs/condow_rusoto
//! [condow_fs]:https://docs.rs/condow_fs
//! [InMemoryClient]:condow_client::InMemoryClient
//! [EnsureActivePull]:config::EnsureActivePull
use futures::future::BoxFuture;

use errors::CondowError;
use probe::Probe;
use streams::{ChunkStream, OrderedChunkStream};

#[macro_use]
pub(crate) mod helpers;
mod condow;
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

pub use condow::Condow;
pub use download_range::*;
pub use request::{Request, RequestNoLocation};

#[cfg(test)]
pub mod test_utils;

/// A common interface for downloading
///
/// This trait is not object safe. If you want to use dynamic dispatch you
/// might consider the trait [DownloadsUntyped] which is object safe
/// and accepts string slices as a location.
///  ```
/// # use std::sync::Arc;
/// use condow_core::{Condow, Downloads, config::Config};
/// use condow_core::condow_client::InMemoryClient;
///
/// # #[tokio::main]
/// # async fn main() {
/// // First we need a client... Let's make the type of the location simply `u32`s
/// let client = InMemoryClient::<i32>::new_static(b"a remote BLOB");
///
/// // ... and a configuration for Condow
/// let config = Config::default();
///
/// let condow = Condow::new(client, config).unwrap();
///
/// assert_eq!(Downloads::get_size(&condow, 42).await.unwrap(), 13);
///
/// let blob = Downloads::blob(&condow).at(42).download_into_vec().await.unwrap();
/// assert_eq!(blob, b"a remote BLOB");
/// # }
/// ```
pub trait Downloads: Send + Sync + 'static {
    type Location: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static;

    /// Download a BLOB via the returned request object
    fn blob(&self) -> RequestNoLocation<Self::Location>;

    /// Get the size of a BLOB at the given location
    fn get_size<'a>(&'a self, location: Self::Location) -> BoxFuture<'a, Result<u64, CondowError>>;
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
}

#[cfg(test)]
mod trait_tests {
    use std::sync::Arc;

    use futures::future::BoxFuture;

    use crate::{
        condow_client::InMemoryClient, config::Config, errors::CondowError,
        request::RequestAdapter, Condow, Downloads, DownloadsUntyped, RequestNoLocation,
    };

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
        }

        let _: Box<dyn DownloadsUntyped> = Box::new(Foo);
    }

    #[tokio::test]
    async fn downloads_untyped_can_create_a_request() {
        struct Foo;

        impl DownloadsUntyped for Foo {
            fn blob(&self) -> RequestNoLocation<&str> {
                struct FooAdapter;

                impl RequestAdapter<&str> for FooAdapter {
                    fn bytes<'a>(
                        &'a self,
                        _location: &str,
                        _params: crate::request::Params,
                    ) -> BoxFuture<'a, Result<crate::streams::BytesStream, CondowError>>
                    {
                        todo!()
                    }

                    fn chunks<'a>(
                        &'a self,
                        _location: &str,
                        _params: crate::request::Params,
                    ) -> BoxFuture<'a, Result<crate::streams::ChunkStream, CondowError>>
                    {
                        todo!()
                    }

                    fn size<'a>(
                        &'a self,
                        _location: &str,
                        _params: crate::request::Params,
                    ) -> BoxFuture<'a, Result<u64, CondowError>> {
                        todo!()
                    }
                }

                RequestNoLocation::new(FooAdapter, Default::default())
            }

            fn get_size<'a>(&'a self, _location: &str) -> BoxFuture<'a, Result<u64, CondowError>> {
                todo!()
            }
        }

        let downloads_untyped: Box<dyn DownloadsUntyped> = Box::new(Foo);

        let _request = downloads_untyped
            .blob()
            .at("42")
            .download_into_vec()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn untyped_downloader_is_usable() {
        let client = InMemoryClient::<u32>::new_static(b"a remote BLOB");
        let config = Config::default();
        let condow = Condow::new(client, config).unwrap();
        let downloader: Arc<dyn DownloadsUntyped> = Arc::new(condow);
        assert_eq!(downloader.get_size("42").await.unwrap(), 13);
        assert!(downloader.get_size("x").await.is_err());
        let blob = downloader
            .blob()
            .at("42")
            .download_into_vec()
            .await
            .unwrap();
        assert_eq!(blob, b"a remote BLOB");
    }

    #[tokio::test]
    async fn typed_downloader_is_usable() {
        let client = InMemoryClient::<u32>::new_static(b"a remote BLOB");
        let config = Config::default();
        let condow = Condow::new(client, config).unwrap();
        let downloader: Arc<dyn Downloads<Location = u32>> = Arc::new(condow);
        assert_eq!(downloader.get_size(42).await.unwrap(), 13);
        let blob = downloader
            .blob()
            .at(42u32)
            .download_into_vec()
            .await
            .unwrap();
        assert_eq!(blob, b"a remote BLOB");
    }
}
