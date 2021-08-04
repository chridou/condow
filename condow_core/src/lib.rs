use condow_client::CondowClient;
use errors::{DownloadFileError, DownloadPartError};

pub mod condow_client;
mod download_range;
pub mod errors;

pub use download_range::DownloadRange;

#[derive(Clone)]
pub struct Condow<C: CondowClient> {
    client: C,
}

impl<C: CondowClient> Condow<C> {
    pub async fn download_file(
        &self,
        location: C::Location,
    ) -> Result<CondowStream, DownloadFileError> {
        self.download_part(location, DownloadRange::Full).await.map_err(DownloadFileError::from)
    }

    pub async fn download_part<R: Into<DownloadRange>>(
        &self,
        location: C::Location,
        range: R,
    ) -> Result<CondowStream, DownloadPartError> {
        let mut range: DownloadRange = range.into();
        range.validate()?;
        range.sanitize();

        if range == DownloadRange::Empty {
            return Ok(CondowStream::empty())
        }

        let size = self.client.get_size(location.clone()).await?;

        if size == 0 {
            return Ok(CondowStream::empty())
        }

        unimplemented!()
    }

    pub fn inner_client(&self) -> &C {
        &self.client
    }
}

pub struct CondowStream {
    n_parts: usize
}

impl CondowStream {
    pub fn empty() -> Self {
        CondowStream {
            n_parts: 0
        }
    }

    pub fn n_parts(&self) -> usize {
        self.n_parts
    } 
}
