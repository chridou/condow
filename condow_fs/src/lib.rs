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
//! let stream = condow.blob().at(String::from("my_file")).range(23..46).download().await.unwrap();
//! let downloaded_bytes: Vec<u8> = stream.into_vec().await.unwrap();
//! # };
//! # ()
//! ```

use std::io::SeekFrom;

use anyhow::Error as AnyError;
use bytes::Bytes;
use condow_core::config::Config;
use futures::future::BoxFuture;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use condow_core::{condow_client::CondowClient, errors::CondowError, streams::BytesStream};

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
        range: InclusiveRange,
    ) -> BoxFuture<'static, Result<BytesStream, CondowError>> {
        let f = async move {
            let bytes = {
                let mut file = fs::File::open(location.as_str()).await?;
                file.seek(SeekFrom::Start(range.start())).await?;

                let n_bytes_to_read = range.len();

                if n_bytes_to_read > usize::MAX as u64 {
                    return Err(CondowError::new_other(
                        "usize overflow while casting from u64",
                    ));
                }

                let mut buffer = vec![0; n_bytes_to_read as usize];

                let n_bytes_read = file.read_exact(&mut buffer).await?;

                if n_bytes_read as u64 != n_bytes_to_read {
                    return Err(CondowError::new_io(format!(
                        "not enough bytes read (expected {} got {})",
                        n_bytes_to_read, n_bytes_read
                    )));
                }

                buffer
            };

            let bytes = Bytes::from(bytes);

            Ok(BytesStream::once_ok(bytes))
        };

        Box::pin(f)
    }
}
