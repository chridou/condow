//! # CONcurrent DOWnloads for local files
//!
//! Load parts of files concurrently.
//!
//! This is mostly for testing and experimenting.
//! In most cases it is better to load sequentially from disks.
//!
//! ```rust, noexec
//!
//! use condow_fs::*;
//! use condow_fs::config::Config;
//!
//! # async {
//! let condow = FsClient::condow(Config::default()).unwrap();
//!
//! let location = String::from("my_file");
//!
//! let stream = condow.download(location, 23..46).await.unwrap();
//! let downloaded_bytes: Vec<u8> = stream.into_vec().await.unwrap();
//! # };
//! # ()
//! ```

use std::io::SeekFrom;

use anyhow::Error as AnyError;
use bytes::Bytes;
use condow_core::config::Config;
use futures::future::BoxFuture;
use futures::StreamExt;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use condow_core::{
    condow_client::{CondowClient, DownloadSpec},
    errors::CondowError,
    streams::{BytesHint, BytesStream},
};

pub use condow_core::*;

#[derive(Clone)]
pub struct FsClient;

impl FsClient {
    /// Create a concurrent downloader from this adapter and the given [Config]
    pub fn condow(config: Config) -> Result<Condow<Self>, AnyError> {
        Condow::new(FsClient, config)
    }
}

impl CondowClient for FsClient {
    type Location = String;

    fn get_size(&self, location: Self::Location) -> BoxFuture<'static, Result<u64, CondowError>> {
        let f = async move {
            let file = fs::File::open(location.as_str()).await?;
            let len = file.metadata().await?.len();

            Ok(len)
        };

        Box::pin(f)
    }

    fn download(
        &self,
        location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>> {
        let f = async move {
            let bytes = match spec {
                DownloadSpec::Complete => fs::read(location.as_str()).await?,
                DownloadSpec::Range(range) => {
                    let mut file = fs::File::open(location.as_str()).await?;
                    file.seek(SeekFrom::Start(range.start() as u64)).await?;

                    let n_bytes_to_read = range.len();
                    let mut buffer = Vec::with_capacity(n_bytes_to_read as usize); // FIX: usize overflow

                    let n_bytes_read = file.read(&mut buffer).await?;

                    if n_bytes_read as u64 != n_bytes_to_read {
                        return Err(CondowError::new_io(format!(
                            "not enough bytes read (expected {} got {})",
                            n_bytes_to_read, n_bytes_read
                        )));
                    }

                    buffer
                }
            };

            let bytes = Bytes::from(bytes);

            let bytes_hint = BytesHint::new_exact(bytes.len() as u64);

            let stream = futures::stream::once(futures::future::ready(Ok(bytes)));

            Ok((stream.boxed(), bytes_hint))
        };

        Box::pin(f)
    }
}
