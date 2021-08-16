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
    errors::{DownloadError, GetSizeError, IoError},
    streams::{BytesHint, BytesStream},
    Condow,
};

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

impl fmt::Display for S3Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.0, self.1)
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
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), DownloadError>> {
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
                return Err(DownloadError::Other("response had no body".to_string()));
            };

            let stream: BytesStream = Box::pin(stream.map_err(|err| IoError(err.to_string())));

            Ok((stream, bytes_hint))
        };

        Box::pin(f)
    }
}

fn get_obj_err_to_download_err(err: RusotoError<GetObjectError>) -> DownloadError {
    match err {
        RusotoError::Service(err) => DownloadError::Remote(format!("Rusoto: {}", err)),
        RusotoError::Validation(cause) => DownloadError::Other(format!("Rusoto: {}", cause)),
        RusotoError::Credentials(err) => DownloadError::Other(format!("Rusoto: {:?}", err)),
        RusotoError::HttpDispatch(dispatch_error) => {
            DownloadError::Other(format!("Rusoto: {:?}", dispatch_error))
        }
        RusotoError::ParseError(cause) => DownloadError::Other(format!("Rusoto: {}", cause)),
        RusotoError::Unknown(cause) => DownloadError::Other(format!("Rusoto: {:?}", cause)),
        RusotoError::Blocking => {
            DownloadError::Other("Rusoto: Failed to run blocking future".into())
        }
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
