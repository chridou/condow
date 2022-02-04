use anyhow::Error as AnyError;
use aws_sdk_s3::{Client, SdkError, error::HeadObjectError, output::HeadObjectOutput};
use futures::{future::BoxFuture, stream::TryStreamExt};

use aws_smithy_client::{
    bounds::{SmithyConnector, SmithyMiddleware},
    retry::NewRequestPolicy, SdkSuccess,
};

use condow_core::{
    condow_client::*,
    config::Config,
    errors::{CondowError, IoError},
    streams::{BytesHint, BytesStream},
};

pub use condow_core::*;
pub use location::*;

mod location;

/// Just a wrapper around a client
/// to implement the trait [CondowClient](condow_client::CondowClient) on.
#[derive(Clone)]
pub struct S3ClientWrapper<C>(C);

impl<C, M, R> S3ClientWrapper<Client<C, M, R>>
where
    C: SmithyConnector,
    M: SmithyMiddleware<C>,
    R: NewRequestPolicy,
{
    pub fn new(client: Client<C, M, R>) -> Self {
        Self(client)
    }

    pub fn client(&self) -> &Client<C, M, R> {
        &self.0
    }
}

impl<C, M, R> CondowClient for S3ClientWrapper<Client<C, M, R>>
where
    C: SmithyConnector + Send + Sync + 'static,
    M: SmithyMiddleware<C> + Send + Sync + 'static,
    R: NewRequestPolicy + Send + Sync + 'static,
{
    type Location = S3Location;

    fn get_size(&self, location: Self::Location) -> BoxFuture<'static, Result<u64, CondowError>> {
        let (bucket, object_key) = location.into_inner();
        let head_obj_req = self
            .0
            .head_object()
            .bucket(bucket.into_inner())
            .key(object_key.into_inner());
        let f = async move {
            let response = head_obj_req
                .send()
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
                range: spec.http_range_value(),
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

            let stream: BytesStream = Box::pin(stream.map_err(|err| IoError(err.to_string())));

            Ok((stream, bytes_hint))
        };

        Box::pin(f)
    }
}
