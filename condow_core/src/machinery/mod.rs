//! Streams for handling downloads
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::condow_client::CondowClient;
use crate::config::Config;
use crate::errors::CondowError;
use crate::streams::{BytesHint, ChunkStream};
use crate::InclusiveRange;
use crate::Reporter;

use self::range_stream::RangeStream;

mod downloaders;
mod range_stream;

pub async fn download<C: CondowClient, R: Reporter>(
    client: C,
    location: C::Location,
    range: InclusiveRange,
    bytes_hint: BytesHint,
    config: Config,
    reporter: R,
) -> Result<ChunkStream, CondowError> {
    reporter.effective_range(range);

    let (n_parts, ranges_stream) = RangeStream::create(range, config.part_size_bytes.into());

    if n_parts == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    let (chunk_stream, sender) = ChunkStream::new(bytes_hint);

    tokio::spawn(async move {
        downloaders::download_concurrently(
            ranges_stream,
            config.max_concurrency.into_inner().min(n_parts),
            sender,
            client,
            config,
            location,
            reporter,
        )
        .await
    });

    Ok(chunk_stream)
}

#[cfg(test)]
mod tests {
    use crate::{
        config::Config, machinery::download, reporter::NoReporter, streams::BytesHint,
        test_utils::*, InclusiveRange,
    };

    #[tokio::test]
    async fn from_0_to_inclusive_range_smaller_than_part_size() {
        let buffer_size = 10;
        let client = TestCondowClient::new().max_chunk_size(3);
        let data = client.data();

        let config = Config::default()
            .buffer_size(buffer_size)
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1);

        let range = InclusiveRange(0, 8);
        let bytes_hint = BytesHint::new(range.len(), Some(range.len()));

        let result_stream = download(client, (), range, bytes_hint, config, NoReporter)
            .await
            .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_std_range()]);
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_equal_size_than_part_size() {
        let buffer_size = 10;
        let client = TestCondowClient::new().max_chunk_size(3);
        let data = client.data();

        let config = Config::default()
            .buffer_size(buffer_size)
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1);

        let range = InclusiveRange(0, 9);
        let bytes_hint = BytesHint::new(range.len(), Some(range.len()));

        let result_stream = download(client, (), range, bytes_hint, config, NoReporter)
            .await
            .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_std_range()]);
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_larger_than_part_size() {
        let buffer_size = 10;
        let client = TestCondowClient::new().max_chunk_size(3);
        let data = client.data();

        let config = Config::default()
            .buffer_size(buffer_size)
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1);

        let range = InclusiveRange(0, 10);
        let bytes_hint = BytesHint::new(range.len(), Some(range.len()));

        let result_stream = download(client, (), range, bytes_hint, config, NoReporter)
            .await
            .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_std_range()]);
    }
}

#[derive(Clone)]
struct KillSwitch {
    is_pushed: Arc<AtomicBool>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            is_pushed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_pushed(&self) -> bool {
        self.is_pushed.load(Ordering::Relaxed)
    }

    pub fn push_the_button(&self) {
        self.is_pushed.store(true, Ordering::Relaxed)
    }
}
