//! # CONcurrent DOWnloads from AWS S3
//!
//! Download speed from S3 can be significantly improved by
//! downloading parts of the file concurrently. This crate
//! does exactly that.
//!
//! Unlike e.g. the AWS Java SDK this library does not download
//! the parts as uploaded but ranges.
use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use anyhow::Error as AnyError;
use futures::{future::BoxFuture, stream::TryStreamExt};
use rusoto_core::{Region, RusotoError};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, HeadObjectError, HeadObjectRequest, S3Client, S3,
};

use condow_core::{
    condow_client::*,
    config::Config,
    errors::{CondowError, GetSizeError, IoError},
    streams::{BytesHint, BytesStream},
};

pub use condow_core::*;

/// S3 bucket name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bucket(String);

impl Bucket {
    pub fn new<T: Into<String>>(bucket: T) -> Self {
        Self(bucket.into())
    }

    pub fn object<O: Into<ObjectKey>>(self, key: O) -> S3Location {
        S3Location(self, key.into())
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
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

/// S3 object key
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new<T: Into<String>>(key: T) -> Self {
        Self(key.into())
    }

    pub fn in_bucket<B: Into<Bucket>>(self, bucket: B) -> S3Location {
        S3Location(bucket.into(), self)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
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

/// Full "path" to an S3 object
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct S3Location(Bucket, ObjectKey);

impl S3Location {
    pub fn new<B: Into<Bucket>, O: Into<ObjectKey>>(bucket: B, key: O) -> Self {
        Self(bucket.into(), key.into())
    }

    pub fn bucket(&self) -> &Bucket {
        &self.0
    }

    pub fn key(&self) -> &ObjectKey {
        &self.1
    }

    /// Turn this into its two components
    pub fn into_inner(self) -> (Bucket, ObjectKey) {
        (self.0, self.1)
    }
}

impl fmt::Display for S3Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.0, self.1)
    }
}

/// Just a wrapper around a clietn
/// to implement the trait [CondowClient](condow_client::CondowClient) on.
#[derive(Clone)]
pub struct S3ClientWrapper<C>(C);

impl S3ClientWrapper<S3Client> {
    /// Create a new wrapper wrapping the default [S3Client](rusoto_s3::S3Client)
    /// for the given [Region](rusoto_core::Region).
    pub fn new(region: Region) -> Self {
        let client = S3Client::new(region);
        Self::from_client(client)
    }
}

impl<C: S3 + Clone + Send + Sync + 'static> S3ClientWrapper<C> {
    /// Create a new wrapper wrapping given an implementor of [S3](rusoto_s3::S3).
    pub fn from_client(client: C) -> Self {
        Self(client)
    }

    /// Create a concurrent downloader from this adapter and the given [Config]
    pub fn condow(self, config: Config) -> Result<Condow<Self>, AnyError> {
        Condow::new(self, config)
    }
}

impl<C: S3 + Clone + Send + Sync + 'static> CondowClient for S3ClientWrapper<C> {
    type Location = S3Location;

    fn get_size(
        &self,
        location: Self::Location,
    ) -> BoxFuture<'static, Result<usize, GetSizeError>> {
        let client = self.0.clone();
        let f = async move {
            let (bucket, object_key) = location.into_inner();
            let head_object_request = HeadObjectRequest {
                bucket: bucket.into_inner(),
                key: object_key.into_inner(),
                ..Default::default()
            };

            let response = client
                .head_object(head_object_request)
                .await
                .map_err(head_obj_err_to_get_size_err)?;

            if let Some(size) = response.content_length {
                Ok(size as usize)
            } else {
                Err(GetSizeError::Other(
                    "response had no content length".to_string(),
                ))
            }
        };

        Box::pin(f)
    }

    fn download(
        &self,
        location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>> {
        let client = self.0.clone();
        let f = async move {
            let (bucket, object_key) = location.into_inner();
            let get_object_request = GetObjectRequest {
                bucket: bucket.into_inner(),
                key: object_key.into_inner(),
                range: spec.http_range_value(),
                ..Default::default()
            };

            let response = client
                .get_object(get_object_request)
                .await
                .map_err(get_obj_err_to_download_err)?;

            let bytes_hint = response
                .content_length
                .map(|s| BytesHint::new_exact(s as usize))
                .unwrap_or_else(BytesHint::new_no_hint);

            let stream = if let Some(stream) = response.body {
                stream
            } else {
                return Err(CondowError::Other("response had no body".to_string()));
            };

            let stream: BytesStream = Box::pin(stream.map_err(|err| IoError(err.to_string())));

            Ok((stream, bytes_hint))
        };

        Box::pin(f)
    }
}

fn get_obj_err_to_download_err(err: RusotoError<GetObjectError>) -> CondowError {
    match err {
        RusotoError::Service(err) => CondowError::Remote(format!("Rusoto: {}", err)),
        RusotoError::Validation(cause) => CondowError::Other(format!("Rusoto: {}", cause)),
        RusotoError::Credentials(err) => CondowError::Other(format!("Rusoto: {:?}", err)),
        RusotoError::HttpDispatch(dispatch_error) => {
            CondowError::Other(format!("Rusoto: {:?}", dispatch_error))
        }
        RusotoError::ParseError(cause) => CondowError::Other(format!("Rusoto: {}", cause)),
        RusotoError::Unknown(cause) => CondowError::Other(format!("Rusoto: {:?}", cause)),
        RusotoError::Blocking => CondowError::Other("Rusoto: Failed to run blocking future".into()),
    }
}

fn head_obj_err_to_get_size_err(err: RusotoError<HeadObjectError>) -> GetSizeError {
    match err {
        RusotoError::Service(err) => GetSizeError::Remote(format!("Rusoto: {}", err)),
        RusotoError::Validation(cause) => GetSizeError::Other(format!("Rusoto: {}", cause)),
        RusotoError::Credentials(err) => GetSizeError::Other(format!("Rusoto: {:?}", err)),
        RusotoError::HttpDispatch(dispatch_error) => {
            GetSizeError::Other(format!("Rusoto: {:?}", dispatch_error))
        }
        RusotoError::ParseError(cause) => GetSizeError::Other(format!("Rusoto: {}", cause)),
        RusotoError::Unknown(cause) => GetSizeError::Other(format!("Rusoto: {:?}", cause)),
        RusotoError::Blocking => {
            GetSizeError::Other("Rusoto: Failed to run blocking future".into())
        }
    }
}
