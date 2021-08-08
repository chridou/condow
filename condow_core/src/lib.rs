use condow_client::CondowClient;
use config::{AlwaysGetSize, Config};
use errors::{DownloadFileError, DownloadRangeError, GetSizeError};

use streams::{BytesHint, BytesStream, ChunkStream};

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

#[derive(Clone)]
pub struct Condow<C> {
    client: C,
    config: Config,
}

impl<C: CondowClient> Condow<C> {
    pub fn new(client: C, config: Config) -> Result<Self, anyhow::Error> {
        let config = config.validated()?;
        Ok(Self { client, config })
    }

    pub async fn download_range<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
        get_size_mode: GetSizeMode,
    ) -> Result<ChunkStream, DownloadRangeError> {
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
                    (range, BytesHint::exact(range.byte_len()))
                } else {
                    return Ok(ChunkStream::empty());
                }
            }
            DownloadRange::Closed(cl) => {
                if get_size_mode.is_load_size_enforced(self.config.always_get_size) {
                    let size = self.client.get_size(location.clone()).await?;
                    if let Some(range) = cl.incl_range_from_size(size) {
                        (range, BytesHint::exact(range.byte_len()))
                    } else {
                        return Ok(ChunkStream::empty());
                    }
                } else {
                    if let Some(range) = cl.incl_range() {
                        (range, BytesHint::at_max(range.byte_len()))
                    } else {
                        return Ok(ChunkStream::empty());
                    }
                }
            }
        };

        if inclusive_range.byte_len() <= self.config.part_size_bytes.into() {
            let (bytes_stream, bytes_hint) = self
                .download_file_non_concurrent(location)
                .await
                .map_err(DownloadRangeError::from)?;
            return Ok(ChunkStream::from_full_file(bytes_stream, bytes_hint));
        }

        machinery::download(
            self.client.clone(),
            location,
            inclusive_range,
            bytes_hint,
            self.config.clone(),
        )
        .await
    }

    pub async fn download_file(
        &self,
        location: C::Location,
    ) -> Result<ChunkStream, DownloadFileError> {
        self.download_range(
            location,
            DownloadRange::Open(OpenRange::Full),
            GetSizeMode::Default,
        )
        .await
        .map_err(DownloadFileError::from)
    }

    pub async fn download_file_non_concurrent(
        &self,
        location: C::Location,
    ) -> Result<(BytesStream, BytesHint), DownloadFileError> {
        self.client
            .download(location, DownloadRange::Open(OpenRange::Full))
            .await
            .map_err(DownloadFileError::from)
    }

    pub async fn get_size(&self, location: C::Location) -> Result<usize, GetSizeError> {
        self.client.get_size(location).await
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GetSizeMode {
    Always,
    Required,
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
mod tests {
    mod file {
        use std::{sync::Arc, time::Duration};

        use crate::{
            config::{Config, Millis},
            test_utils::*,
            Condow,
        };

        #[tokio::test]
        async fn download_file_buff_size_2() {
            let buffer_size = 2;

            let mut data: Vec<u8> = Vec::new();

            for n in 1u8..=255 {
                let bytes = n.to_be_bytes();
                data.extend_from_slice(bytes.as_ref());
            }

            let data = Arc::new(data);

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 1,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay(Millis(1))
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        let result_stream = condow.download_file(()).await.unwrap();

                        let result = result_stream.into_vec().await.unwrap();

                        assert_eq!(&result, data.as_ref());
                    }
                }
            }
        }
    }
}
