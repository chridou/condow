//! # CONcurrent DOWnloads from AWS S3
//!
//! Download speed from S3 can be significantly improved by
//! downloading parts of the file concurrently. This crate
//! does exactly that.
//!
//! Unlike e.g. the AWS Java SDK this library does not download
//! the parts as uploaded but ranges.
//!
//! ```rust, noexec
//!
//! use condow_rusoto::*;
//! use condow_rusoto::config::Config;
//!
//! # async {
//! let client = S3ClientWrapper::new(Region::default());
//! let condow = client.condow(Config::default()).unwrap();
//!
//! let s3_obj = Bucket::new("my_bucket").object("my_object");
//!
//! let stream = condow.blob().at(s3_obj).range(23..46).download().await.unwrap();
//! let downloaded_bytes: Vec<u8> = stream.into_vec().await.unwrap();
//! # };
//! # ()
//! ```
use std::{
    fmt,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use anyhow::{anyhow, Error as AnyError};
use futures::future::BoxFuture;
use rusoto_core::{request::BufferedHttpResponse, RusotoError};
use rusoto_s3::{GetObjectError, GetObjectRequest, HeadObjectError, HeadObjectRequest, S3};

pub use rusoto_core::Region;
pub use rusoto_s3::S3Client;

use condow_core::{
    condow_client::*,
    config::Config,
    errors::CondowError,
    streams::{BytesHint, BytesStream},
};

use condow_core::errors::http_status_to_error;
pub use condow_core::*;

/// S3 bucket name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bucket(String);

impl Bucket {
    pub fn new<T: Into<String>>(bucket: T) -> Self {
        Self(bucket.into())
    }

    /// Specify an object key and make it a [S3Location]
    ///
    /// ```
    /// use condow_rusoto::{Bucket, S3Location};
    ///
    /// let location = Bucket::new("bucket").object("key");
    /// assert_eq!(location, S3Location::new("bucket", "key"));
    /// ```
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

impl From<&str> for Bucket {
    fn from(s: &str) -> Self {
        Self::new(s)
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

    /// Specify a bucket and make it a [S3Location]
    ///
    /// ```
    /// use condow_rusoto::{ObjectKey, S3Location};
    ///
    /// let location = ObjectKey::new("key").in_bucket("bucket");
    /// assert_eq!(location, S3Location::new("bucket", "key"));
    /// ```
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

impl From<&str> for ObjectKey {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

/// Full "path" to an S3 object
///
///
/// ## Examples
///
/// `FromStr`
///
/// ```
/// use condow_rusoto::S3Location;
///
/// let location: S3Location = "s3://my_bucket/my_object".parse().unwrap();
/// assert_eq!(location, S3Location::new("my_bucket", "my_object"));
///
/// let location: S3Location = "s3a://my_bucket/my_prefix/my_object".parse().unwrap();
/// assert_eq!(location, S3Location::new("my_bucket", "my_prefix/my_object"));
///
/// let location: S3Location = "s3n://my_bucket/my_prefix/1/my_object".parse().unwrap();
/// assert_eq!(location, S3Location::new("my_bucket", "my_prefix/1/my_object"));
///
///
/// // invalid protocol
/// assert!("s4://my_bucket/my_object".parse::<S3Location>().is_err());
/// // missing object key
/// assert!("s3://my_bucket".parse::<S3Location>().is_err());
/// // empty object key
/// assert!("s3://my_bucket/".parse::<S3Location>().is_err());
/// // no bucket and object key
/// assert!("s3://".parse::<S3Location>().is_err());
/// // empty bucket
/// assert!("s3:///my_object".parse::<S3Location>().is_err());
/// // no protocol
/// assert!("my_bucket/my_object".parse::<S3Location>().is_err());
/// // no protocol
/// assert!("//my_bucket/my_object".parse::<S3Location>().is_err());
/// // no protocol
/// assert!("//my_bucket/my_object".parse::<S3Location>().is_err());
/// // well... obvious...
/// assert!("".parse::<S3Location>().is_err());
/// ```
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

impl FromStr for S3Location {
    type Err = AnyError;

    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        if uri.is_empty() {
            return Err(anyhow!("S3 URI must no be empty"));
        }

        let prefixes = ["s3://", "s3a://", "s3n://"];
        let res = prefixes
            .iter()
            .find(|&&p| uri.starts_with(p))
            .map(|p| uri.trim_start_matches(p))
            .ok_or_else(|| anyhow!(format!("invalid protocol for S3 URI: '{uri}'")))?;

        let (bucket, key) = res.split_once('/').ok_or_else(|| {
            anyhow!(format!(
                "S3 URI must contain a bucket \
            and object key seperated by a '/': {uri}"
            ))
        })?;

        let bucket = bucket.trim();
        let key = key.trim();

        if bucket.is_empty() {
            return Err(anyhow!("bucket (S3 URI) must no be empty: {uri}"));
        } else if key.is_empty() {
            return Err(anyhow!("object key (S3 URI) must no be empty: {uri}"));
        }

        Ok(Bucket::new(bucket).object(key))
    }
}

/// Just a wrapper around a client
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

    fn get_size(&self, location: Self::Location) -> BoxFuture<'static, Result<u64, CondowError>> {
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
                Ok(size as u64)
            } else {
                Err(CondowError::new_other("response had no content length"))
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
                range: spec.http_bytes_range_value(),
                ..Default::default()
            };

            let response = client
                .get_object(get_object_request)
                .await
                .map_err(get_obj_err_to_download_err)?;

            let bytes_hint = response
                .content_length
                .map(|s| BytesHint::new_exact(s as u64))
                .unwrap_or_else(BytesHint::new_no_hint);

            let stream = if let Some(stream) = response.body {
                stream
            } else {
                return Err(CondowError::new_other("response had no body"));
            };

            let stream: BytesStream = Box::pin(stream);

            Ok((stream, bytes_hint))
        };

        Box::pin(f)
    }
}

fn get_obj_err_to_download_err(err: RusotoError<GetObjectError>) -> CondowError {
    match err {
        RusotoError::Service(err) => match err {
            GetObjectError::NoSuchKey(s) => CondowError::new_not_found(s),
            GetObjectError::InvalidObjectState(s) => {
                CondowError::new_other(format!("invalid object state (get object request): {}", s))
            }
        },
        RusotoError::Validation(cause) => {
            CondowError::new_other(format!("validation error (get object request): {}", cause))
        }
        RusotoError::Credentials(err) => {
            CondowError::new_other(format!("credentials error (get object request): {}", err))
                .with_source(err)
        }
        RusotoError::HttpDispatch(dispatch_error) => CondowError::new_other(format!(
            "http dispatch error (get object request): {}",
            dispatch_error
        ))
        .with_source(dispatch_error),
        RusotoError::ParseError(cause) => {
            CondowError::new_other(format!("parse error (get object request): {}", cause))
        }
        RusotoError::Unknown(response) => response_to_condow_err(response),
        RusotoError::Blocking => {
            CondowError::new_other("failed to run blocking future within rusoto")
        }
    }
}

fn head_obj_err_to_get_size_err(err: RusotoError<HeadObjectError>) -> CondowError {
    match err {
        RusotoError::Service(err) => match err {
            HeadObjectError::NoSuchKey(s) => CondowError::new_not_found(s),
        },
        RusotoError::Validation(cause) => {
            CondowError::new_other(format!("validation error (head object request): {}", cause))
        }
        RusotoError::Credentials(err) => {
            CondowError::new_other(format!("credentials error (head object request): {}", err))
                .with_source(err)
        }
        RusotoError::HttpDispatch(dispatch_error) => CondowError::new_other(format!(
            "http dispatch error (head object request): {}",
            dispatch_error
        ))
        .with_source(dispatch_error),
        RusotoError::ParseError(cause) => {
            CondowError::new_other(format!("parse error (head object request): {}", cause))
        }
        RusotoError::Unknown(response) => response_to_condow_err(response),
        RusotoError::Blocking => {
            CondowError::new_other("failed to run blocking future within rusoto")
        }
    }
}

fn response_to_condow_err(response: BufferedHttpResponse) -> CondowError {
    let s = response.status;
    http_status_to_error(
        s.as_u16(),
        &s.to_string(),
        s.is_server_error(),
        response.body.as_ref(),
    )
}
