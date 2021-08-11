use crate::condow_client::CondowClient;
use crate::config::Config;
use crate::errors::DownloadRangeError;
use crate::streams::{BytesHint, ChunkStream};
use crate::InclusiveRange;

use self::range_stream::RangeStream;

mod downloader;
mod range_stream;

pub async fn download<C: CondowClient>(
    client: C,
    location: C::Location,
    range: InclusiveRange,
    bytes_hint: BytesHint,
    config: Config,
) -> Result<ChunkStream, DownloadRangeError> {
    let (n_parts, ranges_stream) = RangeStream::create(range, config.part_size_bytes.into());

    if n_parts == 0 {
        panic!("n_parts must not be 0. This is a bug");
    }

    let (chunk_stream, sender) = ChunkStream::new(n_parts, bytes_hint);

    tokio::spawn(async move {
        downloader::download_concurrently(
            ranges_stream,
            config.max_concurrency.into_inner().min(n_parts),
            sender,
            client,
            config,
            location,
        )
        .await
    });

    Ok(chunk_stream)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        config::Config, machinery::download, streams::BytesHint, test_utils::create_test_data,
        test_utils::*, InclusiveRange,
    };

    #[tokio::test]
    async fn from_0_to_inclusive_range_smaller_than_part_size() {
        let buffer_size = 10;

        let data = Arc::new(create_test_data());

        let client = TestCondowClient {
            data: Arc::clone(&data),
            max_jitter_ms: 0,
            include_size_hint: true,
            max_chunk_size: 3,
        };

        let config = Config::default()
            .buffer_size(buffer_size)
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1);

        let range = InclusiveRange(0, 8);
        let bytes_hint = BytesHint::new(range.len(), Some(range.len()));

        let result_stream = download(client, (), range, bytes_hint, config)
            .await
            .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_range()]);
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_equal_size_than_part_size() {
        let buffer_size = 10;

        let data = Arc::new(create_test_data());

        let client = TestCondowClient {
            data: Arc::clone(&data),
            max_jitter_ms: 0,
            include_size_hint: true,
            max_chunk_size: 3,
        };

        let config = Config::default()
            .buffer_size(buffer_size)
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1);

        let range = InclusiveRange(0, 9);
        let bytes_hint = BytesHint::new(range.len(), Some(range.len()));

        let result_stream = download(client, (), range, bytes_hint, config)
            .await
            .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_range()]);
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_larger_than_part_size() {
        let buffer_size = 10;

        let data = Arc::new(create_test_data());

        let client = TestCondowClient {
            data: Arc::clone(&data),
            max_jitter_ms: 0,
            include_size_hint: true,
            max_chunk_size: 3,
        };

        let config = Config::default()
            .buffer_size(buffer_size)
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1);

        let range = InclusiveRange(0, 10);
        let bytes_hint = BytesHint::new(range.len(), Some(range.len()));

        let result_stream = download(client, (), range, bytes_hint, config)
            .await
            .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_range()]);
    }
}
