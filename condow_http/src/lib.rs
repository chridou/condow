//! # CONcurrent DOWnloads for local files
//!
//! Load parts of files concurrently.
//!
//! This is mostly for testing and experimenting.
//! In most cases it is better to load sequentially from disks.
//!
//! ```rust, noexec
//!
//! use condow_http::*;
//! use condow_http::config::Config;
//!
//! # async {
//! let condow = HttpClient::condow(Config::default()).unwrap();
//!
//! let location = String::from("my_file");
//!
//! let stream = condow.blob().at(location).range(23..46).download().await.unwrap();
//! let downloaded_bytes: Vec<u8> = stream.into_vec().await.unwrap();
//! # };
//! # ()
//! ```
use std::str::FromStr;

use anyhow::Error as AnyError;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use http_content_range::ContentRange;
use reqwest::{
    header::HeaderMap, header::HeaderName, header::CONTENT_LENGTH, header::CONTENT_RANGE,
    header::RANGE, Client, StatusCode,
};

use condow_core::config::Config;
use condow_core::errors::http_status_to_error;
pub use condow_core::*;
use condow_core::{
    condow_client::{ClientBytesStream, CondowClient},
    errors::CondowError,
};

#[derive(Clone)]
pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    /// Create a concurrent downloader from this adapter and the given [Config]
    pub fn condow(config: Config) -> Result<Condow<Self>, AnyError> {
        let client = reqwest::Client::builder().build()?;
        Self::condow_with(config, client)
    }

    /// Create a concurrent downloader from this adapter and the given [Config], and pass in [Client].
    pub fn condow_with(config: Config, client: Client) -> Result<Condow<Self>, AnyError> {
        Condow::new(HttpClient { client }, config)
    }
}

impl CondowClient for HttpClient {
    type Location = String;

    fn get_size(&self, location: Self::Location) -> BoxFuture<'static, Result<u64, CondowError>> {
        let client = self.client.clone();
        Box::pin(async move {
            // TODO: implement timeout support, e.g. head(url).timeout(config.timeout).send()
            let res = client
                .head(location)
                .send()
                .await
                .map_err(reqwest_error_to_condow_error)?;
            parse_content_length(res.headers())
        })
    }

    fn download(
        &self,
        location: Self::Location,
        range: InclusiveRange,
    ) -> BoxFuture<'static, Result<ClientBytesStream, CondowError>> {
        dbg!(range);
        let client = self.client.clone();
        Box::pin(async move {
            // TODO: implement timeout support, e.g. head(url).timeout(config.timeout).send()
            let mut req = client.get(location);
            req = req.header(RANGE, range.http_bytes_range_value());
            let res = req.send().await.map_err(reqwest_error_to_condow_error)?;
            let status = res.status();
            if status.is_success() {
                let hint = if status == StatusCode::PARTIAL_CONTENT {
                    let range = header_as_str(res.headers(), &CONTENT_RANGE)?;
                    match ContentRange::parse(range) {
                        ContentRange::Bytes(r) => (r.last_byte - r.first_byte + 1) as u64,
                        _ => range.len() as u64,
                    }
                } else {
                    parse_content_length(res.headers())?
                };
                let stream = res.bytes_stream().map_err(reqwest_error_to_condow_error);

                let stream = ClientBytesStream::new(stream, hint);

                Ok(stream)
            } else {
                Err(http_status_to_error(
                    status.as_u16(),
                    &status.to_string(),
                    status.is_server_error(),
                    res.bytes().await.unwrap_or_default().as_ref(),
                ))
            }
        })
    }
}

fn parse_content_length(headers: &HeaderMap) -> Result<u64, CondowError> {
    // NOTE: res.content_length() might give incorrect value, so parsing it directly
    // See https://github.com/seanmonstar/reqwest/issues/843
    let len = header_as_str(headers, &CONTENT_LENGTH)?;
    u64::from_str(len).map_err(|_| {
        CondowError::new_other(format!("{} header is not a valid integer", CONTENT_LENGTH))
    })
}

fn header_as_str<'a>(headers: &'a HeaderMap, header: &HeaderName) -> Result<&'a str, CondowError> {
    let len = headers
        .get(header)
        .ok_or_else(|| CondowError::new_other(format!("{} header is not available", header)))?;
    len.to_str()
        .map_err(|_| CondowError::new_other(format!("{} header is not a valid string", header)))
}

fn reqwest_error_to_condow_error(err: reqwest::Error) -> CondowError {
    if err.is_connect() {
        CondowError::new_io("connect failed").with_source(err)
    } else if err.is_request() {
        CondowError::new_io("request failed").with_source(err)
    } else if err.is_timeout() {
        CondowError::new_io("send HTTP request failed").with_source(err)
    } else {
        CondowError::new_other("reqwest error").with_source(err)
    }
}
