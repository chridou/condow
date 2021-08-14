use std::ops::{Deref, DerefMut};

use anyhow::Error as AnyError;
use futures::future::BoxFuture;
use rusoto_core::Region;
use rusoto_s3::{S3Client, S3};

use condow_core::{condow_client::*, config::Config, Condow};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bucket(String);

impl Bucket {
    pub fn new<T: Into<String>>(bucket: T) -> Self {
        Self(bucket.into())
    }

    pub fn object(self, key: ObjectKey) -> S3Location {
        S3Location(self, key)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Deref for Bucket {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Bucket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new<T: Into<String>>(key: T) -> Self {
        Self(key.into())
    }

    pub fn in_bucket(self, bucket: Bucket) -> S3Location {
        S3Location(bucket, self)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Deref for ObjectKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ObjectKey {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct S3Location(Bucket, ObjectKey);

impl S3Location {
    pub fn new(bucket: Bucket, key: ObjectKey) -> Self {
        Self(bucket, key)
    }

    pub fn bucket(&self) -> &Bucket {
        &self.0
    }

    pub fn key(&self) -> &ObjectKey {
        &self.1
    }

    pub fn into_inner(self) -> (Bucket, ObjectKey) {
        (self.0, self.1)
    }
}

#[derive(Clone)]
pub struct S3ClientWrapper<C>(C);

impl S3ClientWrapper<S3Client> {
    pub fn new(region: Region) -> Self {
        let client = S3Client::new(region);
        Self::from_client(client)
    }
}

impl<C: S3 + Clone + Send + Sync + 'static> S3ClientWrapper<C> {
    pub fn from_client(client: C) -> Self {
        Self(client)
    }

    pub fn condow(self, config: Config) -> Result<Condow<Self>, AnyError> {
        Condow::new(self, config)
    }
}

impl<C: S3 + Clone + Send + Sync + 'static> CondowClient for S3ClientWrapper<C> {
    type Location = S3Location;

    fn get_size(
        &self,
        location: Self::Location,
    ) -> BoxFuture<'static, Result<usize, condow_core::errors::GetSizeError>> {
        todo!()
    }

    fn download(
        &self,
        location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<
        'static,
        Result<
            (
                condow_core::streams::BytesStream,
                condow_core::streams::BytesHint,
            ),
            condow_core::errors::DownloadError,
        >,
    > {
        todo!()
    }
}
