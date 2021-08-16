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
use condow_client::CondowClient;
use config::{AlwaysGetSize, Config};
use errors::{CondowError, GetSizeError};

use streams::{BytesHint, ChunkStream, PartStream};

#[macro_use]
pub(crate) mod helpers;
pub mod condow_client;
pub mod config;
mod download_range;
pub mod errors;
mod machinery;
pub mod streams;

pub use download_range::*;

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
#[derive(Clone)]
pub struct Condow<C> {
    client: C,
    config: Config,
}

impl<C: CondowClient> Condow<C> {
    /// Create a new CONcurrent DOWnloader.
    ///
    /// Fails if the [Config] is not valid.
    pub fn new(client: C, config: Config) -> Result<Self, anyhow::Error> {
        let config = config.validated()?;
        Ok(Self { client, config })
    }

    /// Create a reusable [Downloader] which is just an alternate form to use the API.
    pub fn downloader(&self, location: C::Location) -> Downloader<C> {
        Downloader::new(self.clone(), location)
    }

    /// Download a file (potentially) concurrently
    ///
    /// Returns a stream of [Chunk](streams::Chunk)s.
    /// The [Chunk](streams::Chunk)s are ordered for each individually downloaded
    /// part of the whole download. But the parts themselves are downloaded
    /// with no defined ordering due to the concurrency.
    pub async fn download_chunks<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
        get_size_mode: GetSizeMode,
    ) -> Result<ChunkStream, CondowError> {
        let range: DownloadRange = range.into();
        range.validate()?;
        let range = if let Some(range) = range.sanitized() {
            range
        } else {
            return Ok(ChunkStream::empty());
        };

        let (inclusive_range, bytes_hint) = match range {
            DownloadRange::Open(or) => {
                let size = self.client.get_size(location.clone()).await?;
                if let Some(range) = or.incl_range_from_size(size) {
                    (range, BytesHint::new_exact(range.len()))
                } else {
                    return Ok(ChunkStream::empty());
                }
            }
            DownloadRange::Closed(cl) => {
                if get_size_mode.is_load_size_enforced(self.config.always_get_size) {
                    let size = self.client.get_size(location.clone()).await?;
                    if let Some(range) = cl.incl_range_from_size(size) {
                        (range, BytesHint::new_exact(range.len()))
                    } else {
                        return Ok(ChunkStream::empty());
                    }
                } else if let Some(range) = cl.incl_range() {
                    (range, BytesHint::new_at_max(range.len()))
                } else {
                    return Ok(ChunkStream::empty());
                }
            }
        };

        machinery::download(
            self.client.clone(),
            location,
            inclusive_range,
            bytes_hint,
            self.config.clone(),
        )
        .await
    }

    /// Get the size of a file at the given location
    pub async fn get_size(&self, location: C::Location) -> Result<usize, GetSizeError> {
        self.client.get_size(location).await
    }
}

/// A configured downloader.
///
/// This struct has state which configures a download.
pub struct Downloader<C: CondowClient> {
    /// The location of the file to be downloaded
    pub location: C::Location,
    /// The range of the fle to be downloaded
    ///
    /// Default: Download the whole BLOB
    pub range: DownloadRange,
    /// Mode for handling upper bounds of a range and open ranges
    ///
    /// Default: As configured with [Condow] itself
    pub get_size_mode: GetSizeMode,
    condow: Condow<C>,
}

impl<C: CondowClient> Downloader<C> {
    pub fn new(condow: Condow<C>, location: C::Location) -> Self {
        Self {
            condow,
            location,
            range: DownloadRange::Open(OpenRange::Full),
            get_size_mode: GetSizeMode::default(),
        }
    }

    /// Change the location of the download
    pub fn location(mut self, location: C::Location) -> Self {
        self.location = location;
        self
    }

    /// Set the range of the download
    pub fn range<T: Into<DownloadRange>>(mut self, range: T) -> Self {
        self.range = range.into();
        self
    }

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
    pub async fn download_chunks(&self) -> Result<ChunkStream, CondowError> {
        self.condow
            .download_chunks(self.location.clone(), self.range, self.get_size_mode)
            .await
    }

    /// Download the BLOB/range.
    ///
    /// The parts and the chunks streamed have the same ordering as
    /// within the BLOB/range downloaded.
    ///
    /// See also [download](Condow::download)
    pub async fn download(&self) -> Result<PartStream<ChunkStream>, CondowError> {
        let chunk_stream = self.download_chunks().await?;
        PartStream::from_chunk_stream(chunk_stream)
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
mod condow_tests {
    mod file {
        use std::sync::Arc;

        use crate::{config::Config, test_utils::*, Condow};

        use crate::test_utils::create_test_data;

        #[tokio::test]
        async fn download_blob() {
            let buffer_size = 10;

            let data = Arc::new(create_test_data());

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 0,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        let result_stream = condow
                            .download_chunks((), .., Default::default())
                            .await
                            .unwrap();

                        let result = result_stream.into_vec().await.unwrap();

                        assert_eq!(&result, data.as_ref());
                    }
                }
            }
        }
    }

    mod range {
        mod open {
            use std::sync::Arc;

            use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

            #[tokio::test]
            async fn from_always_get_size() {
                let buffer_size = 10;

                let data = Arc::new(create_test_data());

                for chunk_size in [1, 3, 5] {
                    let client = TestCondowClient {
                        data: Arc::clone(&data),
                        max_jitter_ms: 0,
                        include_size_hint: true,
                        max_chunk_size: chunk_size,
                    };

                    for part_size in [1usize, 3, 50, 1_000] {
                        for n_concurrency in [1usize, 10] {
                            let config = Config::default()
                                .buffer_size(buffer_size)
                                .buffers_full_delay_ms(0)
                                .part_size_bytes(part_size)
                                .max_concurrency(n_concurrency);
                            let condow = Condow::new(client.clone(), config).unwrap();

                            for from_idx in [0usize, 101, 255, 256] {
                                let range = from_idx..;

                                let result_stream = condow
                                    .download_chunks((), range.clone(), crate::GetSizeMode::Always)
                                    .await
                                    .unwrap();

                                let result = result_stream.into_vec().await.unwrap();

                                let check_range = from_idx.min(data.len())..;
                                assert_eq!(&result, &data[check_range]);
                            }
                        }
                    }
                }
            }

            #[tokio::test]
            async fn from_when_required_get_size() {
                let buffer_size = 10;

                let data = Arc::new(create_test_data());

                for chunk_size in [1, 3, 5] {
                    let client = TestCondowClient {
                        data: Arc::clone(&data),
                        max_jitter_ms: 0,
                        include_size_hint: true,
                        max_chunk_size: chunk_size,
                    };

                    for part_size in [1usize, 3, 50, 1_000] {
                        for n_concurrency in [1usize, 10] {
                            let config = Config::default()
                                .buffer_size(buffer_size)
                                .buffers_full_delay_ms(0)
                                .part_size_bytes(part_size)
                                .max_concurrency(n_concurrency);
                            let condow = Condow::new(client.clone(), config).unwrap();

                            for from_idx in [0usize, 101, 255, 256] {
                                let range = from_idx..;

                                let result_stream = condow
                                    .download_chunks(
                                        (),
                                        range.clone(),
                                        crate::GetSizeMode::Required,
                                    )
                                    .await
                                    .unwrap();

                                let result = result_stream.into_vec().await.unwrap();

                                let check_range = from_idx.min(data.len())..;
                                assert_eq!(&result, &data[check_range]);
                            }
                        }
                    }
                }
            }
        }

        mod closed {
            use std::sync::Arc;

            use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

            #[tokio::test]
            async fn to_inclusive() {
                let buffer_size = 10;

                let data = Arc::new(create_test_data());

                for chunk_size in [1, 3, 5] {
                    let client = TestCondowClient {
                        data: Arc::clone(&data),
                        max_jitter_ms: 0,
                        include_size_hint: true,
                        max_chunk_size: chunk_size,
                    };

                    for part_size in [1usize, 3, 50, 1_000] {
                        for n_concurrency in [1usize, 10] {
                            let config = Config::default()
                                .buffer_size(buffer_size)
                                .buffers_full_delay_ms(0)
                                .part_size_bytes(part_size)
                                .max_concurrency(n_concurrency);
                            let condow = Condow::new(client.clone(), config).unwrap();

                            for end_incl in [0usize, 2, 101, 255] {
                                let range = 0..=end_incl;
                                let expected_range_end = (end_incl + 1).min(data.len());

                                let result_stream = condow
                                    .download_chunks((), range.clone(), crate::GetSizeMode::Default)
                                    .await
                                    .unwrap();

                                let result = result_stream.into_vec().await.unwrap();

                                assert_eq!(&result, &data[0..expected_range_end]);
                            }
                        }
                    }
                }
            }

            #[tokio::test]
            async fn to_exclusive() {
                let buffer_size = 10;

                let data = Arc::new(create_test_data());

                for chunk_size in [1, 3, 5] {
                    let client = TestCondowClient {
                        data: Arc::clone(&data),
                        max_jitter_ms: 0,
                        include_size_hint: true,
                        max_chunk_size: chunk_size,
                    };

                    for part_size in [1usize, 3, 50, 1_000] {
                        for n_concurrency in [1usize, 10] {
                            let config = Config::default()
                                .buffer_size(buffer_size)
                                .buffers_full_delay_ms(0)
                                .part_size_bytes(part_size)
                                .max_concurrency(n_concurrency);
                            let condow = Condow::new(client.clone(), config).unwrap();

                            for end_excl in [0usize, 2, 101, 255, 256] {
                                let range = 0..end_excl;
                                let expected_range_end = end_excl.min(data.len());

                                let result_stream = condow
                                    .download_chunks((), range.clone(), crate::GetSizeMode::Default)
                                    .await
                                    .unwrap();

                                let result = result_stream.into_vec().await.unwrap();

                                assert_eq!(&result, &data[0..expected_range_end]);
                            }
                        }
                    }
                }
            }

            mod from_to {
                mod start_at_0 {
                    use std::sync::Arc;

                    use crate::{
                        config::Config, test_utils::create_test_data, test_utils::*, Condow,
                    };

                    #[tokio::test]
                    async fn from_0_to_inclusive() {
                        let buffer_size = 10;

                        let data = Arc::new(create_test_data());

                        for chunk_size in [1, 3, 5] {
                            let client = TestCondowClient {
                                data: Arc::clone(&data),
                                max_jitter_ms: 0,
                                include_size_hint: true,
                                max_chunk_size: chunk_size,
                            };

                            for part_size in [1usize, 3, 50, 1_000] {
                                for n_concurrency in [1usize, 10] {
                                    let config = Config::default()
                                        .buffer_size(buffer_size)
                                        .buffers_full_delay_ms(0)
                                        .part_size_bytes(part_size)
                                        .max_concurrency(n_concurrency);
                                    let condow = Condow::new(client.clone(), config).unwrap();

                                    for end_incl in [0usize, 2, 101, 255, 255] {
                                        let range = 0..=end_incl;
                                        let expected_range_end = (end_incl + 1).min(data.len());

                                        let result_stream = condow
                                            .download_chunks((), range, crate::GetSizeMode::Default)
                                            .await
                                            .unwrap();

                                        let result = result_stream.into_vec().await.unwrap();

                                        assert_eq!(&result, &data[0..expected_range_end]);
                                    }
                                }
                            }
                        }
                    }

                    #[tokio::test]
                    async fn from_0_to_exclusive() {
                        let buffer_size = 10;

                        let data = Arc::new(create_test_data());

                        for chunk_size in [1, 3, 5] {
                            let client = TestCondowClient {
                                data: Arc::clone(&data),
                                max_jitter_ms: 0,
                                include_size_hint: true,
                                max_chunk_size: chunk_size,
                            };

                            for part_size in [1usize, 3, 50, 1_000] {
                                for n_concurrency in [1usize, 10] {
                                    let config = Config::default()
                                        .buffer_size(buffer_size)
                                        .buffers_full_delay_ms(0)
                                        .part_size_bytes(part_size)
                                        .max_concurrency(n_concurrency);
                                    let condow = Condow::new(client.clone(), config).unwrap();

                                    for end_excl in [0usize, 2, 101, 255, 256] {
                                        let range = 0..end_excl;
                                        let expected_range_end = end_excl.min(data.len());

                                        let result_stream = condow
                                            .download_chunks((), range, crate::GetSizeMode::Default)
                                            .await
                                            .unwrap();

                                        let result = result_stream.into_vec().await.unwrap();

                                        assert_eq!(&result, &data[0..expected_range_end]);
                                    }
                                }
                            }
                        }
                    }
                }

                mod start_after_0 {
                    use std::sync::Arc;

                    use crate::{
                        config::Config, test_utils::create_test_data, test_utils::*, Condow,
                    };

                    #[tokio::test]
                    async fn from_to_inclusive() {
                        let buffer_size = 10;

                        let data = Arc::new(create_test_data());

                        for chunk_size in [3] {
                            let client = TestCondowClient {
                                data: Arc::clone(&data),
                                max_jitter_ms: 0,
                                include_size_hint: true,
                                max_chunk_size: chunk_size,
                            };

                            for part_size in [1usize, 33] {
                                for n_concurrency in [1usize, 10] {
                                    let config = Config::default()
                                        .buffer_size(buffer_size)
                                        .buffers_full_delay_ms(0)
                                        .part_size_bytes(part_size)
                                        .max_concurrency(n_concurrency);
                                    let condow = Condow::new(client.clone(), config).unwrap();

                                    for start in [1usize, 87, 101, 201] {
                                        for len in [1, 10, 100] {
                                            let end_incl = start + len;
                                            let range = start..=(end_incl);
                                            let expected_range_end = (end_incl + 1).min(data.len());

                                            let result_stream = condow
                                                .download_chunks(
                                                    (),
                                                    range,
                                                    crate::GetSizeMode::Default,
                                                )
                                                .await
                                                .unwrap();

                                            let result = result_stream.into_vec().await.unwrap();

                                            assert_eq!(&result, &data[start..expected_range_end]);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    #[tokio::test]
                    async fn from_to_exclusive() {
                        let buffer_size = 10;

                        let data = Arc::new(create_test_data());

                        for chunk_size in [3] {
                            let client = TestCondowClient {
                                data: Arc::clone(&data),
                                max_jitter_ms: 0,
                                include_size_hint: true,
                                max_chunk_size: chunk_size,
                            };

                            for part_size in [1usize, 33] {
                                for n_concurrency in [1usize, 10] {
                                    let config = Config::default()
                                        .buffer_size(buffer_size)
                                        .buffers_full_delay_ms(0)
                                        .part_size_bytes(part_size)
                                        .max_concurrency(n_concurrency);
                                    let condow = Condow::new(client.clone(), config).unwrap();

                                    for start in [1usize, 87, 101, 201] {
                                        for len in [1, 10, 100] {
                                            let end_excl = start + len;
                                            let range = start..(end_excl);
                                            let expected_range_end = end_excl.min(data.len());

                                            let result_stream = condow
                                                .download_chunks(
                                                    (),
                                                    range,
                                                    crate::GetSizeMode::Default,
                                                )
                                                .await
                                                .unwrap();

                                            let result = result_stream.into_vec().await.unwrap();

                                            assert_eq!(&result, &data[start..expected_range_end]);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
