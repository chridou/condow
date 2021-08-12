
use rusoto_core::{Client, Region};
use rusoto_s3::S3;
use anyhow::Error as AnyError;

use condow_core::condow_client::*;


#[derive(Debug, Clone)]
pub struct Bucket(String);

#[derive(Debug, Clone)]
pub struct ObjectKey(String);



#[derive(Debug, Clone)]
pub struct S3Location(Bucket, ObjectKey);

pub struct S3ClientWrapper<C>(C);

impl<C: S3 + Clone + Send + Sync + 'static> S3ClientWrapper<C> {
    pub fn new(region: Region) -> Self {
        let client = Client::new(region);
        Self::from_client(client)
    }

    pub fn from_client(client: C) -> Self {
        Self(client)
    }

    pub fn condow(self, config: Config) -> Result<Condow<Self>, AnyError> {
        Condow::new(self, config)
    }
}

impl<C: S3 + Clone + Send + Sync + 'static> CondowClient for S3ClientWrapper<C> {
    type Location;

    fn get_size(&self, location: Self::Location)
        -> BoxFuture<'static, Result<usize, condow_core::errors::GetSizeError>> {
        todo!()
    }

    fn download(
        &self,
        location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(condow_core::streams::BytesStream, condow_core::streams::BytesHint), condow_core::errors::DownloadError>> {
        todo!()
    }
}