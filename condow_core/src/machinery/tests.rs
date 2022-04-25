mod download {
    use crate::{
        condow_client::{failing_client_simulator::FailingClientSimulatorBuilder, NoLocation},
        config::Config,
        errors::CondowErrorKind,
        machinery::download_range,
    };

    #[tokio::test]
    async fn download_ok() {
        let blob = (0u8..100).collect::<Vec<_>>();

        let config = Config::default()
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(2)
            .disable_retries();

        let condow = FailingClientSimulatorBuilder::default()
            .blob(blob.clone())
            .finish()
            .condow(config)
            .unwrap();

        let result = download_range(
            condow,
            NoLocation,
            0..100,
            crate::GetSizeMode::Required,
            Default::default(),
        )
        .await;

        let stream = result.unwrap();

        let result_bytes = stream.into_vec().await.unwrap();

        assert_eq!(result_bytes, blob);
    }

    #[tokio::test]
    async fn download_request_failure() {
        let blob = (0u8..100).collect::<Vec<_>>();

        let config = Config::default()
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(2)
            .disable_retries();

        let condow = FailingClientSimulatorBuilder::default()
            .blob(blob.clone())
            .responses()
            .failure(CondowErrorKind::Other)
            .finish()
            .condow(config)
            .unwrap();

        let result = download_range(
            condow,
            NoLocation,
            0..100,
            crate::GetSizeMode::Required,
            Default::default(),
        )
        .await;

        let stream = result.unwrap();

        assert_eq!(
            stream.into_vec().await.unwrap_err().kind(),
            CondowErrorKind::Other
        );
    }

    #[tokio::test]
    async fn download_stream_error() {
        let blob = (0u8..100).collect::<Vec<_>>();

        let config = Config::default()
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(2)
            .disable_retries();

        let condow = FailingClientSimulatorBuilder::default()
            .blob(blob.clone())
            .responses()
            .success_with_stream_failure(5)
            .finish()
            .condow(config)
            .unwrap();

        let result = download_range(
            condow,
            NoLocation,
            0..100,
            crate::GetSizeMode::Required,
            Default::default(),
        )
        .await;

        let stream = result.unwrap();

        assert_eq!(
            stream.into_vec().await.unwrap_err().kind(),
            CondowErrorKind::Io
        );
    }

    #[tokio::test]
    async fn download_panic_no_retries() {
        let blob = (0u8..100).collect::<Vec<_>>();

        let config = Config::default()
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(2)
            .disable_retries();

        let condow = FailingClientSimulatorBuilder::default()
            .blob(blob.clone())
            .responses()
            .panic("BAMM!")
            .finish()
            .condow(config)
            .unwrap();

        let result = download_range(
            condow,
            NoLocation,
            0..100,
            crate::GetSizeMode::Required,
            Default::default(),
        )
        .await;

        let stream = result.unwrap();

        let err = stream.into_vec().await.unwrap_err();

        assert_eq!(err.kind(), CondowErrorKind::Other);

        assert_eq!(err.msg(), "download ended unexpectedly due to a panic");
    }

    #[tokio::test]
    async fn download_panic_in_retry_after_stream_failure() {
        let blob = (0u8..100).collect::<Vec<_>>();

        let config = Config::default()
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1)
            .configure_retries(|rc| rc.max_attempts(1).initial_delay_ms(0));

        let condow = FailingClientSimulatorBuilder::default()
            .blob(blob.clone())
            .responses()
            .success_with_stream_failure(5)
            .panic("BAMM!")
            .never()
            .finish()
            .condow(config)
            .unwrap();

        let result = download_range(
            condow,
            NoLocation,
            0..100,
            crate::GetSizeMode::Required,
            Default::default(),
        )
        .await;

        let stream = result.unwrap();

        let err = stream.into_vec().await.unwrap_err();

        assert_eq!(err.kind(), CondowErrorKind::Io);

        assert_eq!(err.msg(), "panicked while retrying");
    }

    #[tokio::test]
    async fn download_panic_while_streaming() {
        let blob = (0u8..100).collect::<Vec<_>>();

        let config = Config::default()
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(2)
            .disable_retries();

        let condow = FailingClientSimulatorBuilder::default()
            .blob(blob.clone())
            .responses()
            .success_with_stream_panic(5)
            .finish()
            .condow(config)
            .unwrap();

        let result = download_range(
            condow,
            NoLocation,
            0..100,
            crate::GetSizeMode::Required,
            Default::default(),
        )
        .await;

        let stream = result.unwrap();

        let err = stream.into_vec().await.unwrap_err();

        assert_eq!(err.kind(), CondowErrorKind::Other);

        assert_eq!(err.msg(), "download ended unexpectedly due to a panic");
    }

    #[tokio::test]
    async fn download_panic_in_retry_while_streaming() {
        let blob = (0u8..100).collect::<Vec<_>>();

        let config = Config::default()
            .buffers_full_delay_ms(0)
            .part_size_bytes(10)
            .max_concurrency(1)
            .configure_retries(|rc| rc.max_attempts(1).initial_delay_ms(0));

        let condow = FailingClientSimulatorBuilder::default()
            .blob(blob.clone())
            .responses()
            .success_with_stream_failure(5)
            .success_with_stream_panic(5)
            .never()
            .finish()
            .condow(config)
            .unwrap();

        let result = download_range(
            condow,
            NoLocation,
            0..100,
            crate::GetSizeMode::Required,
            Default::default(),
        )
        .await;

        let stream = result.unwrap();

        let err = stream.into_vec().await.unwrap_err();

        assert_eq!(err.kind(), CondowErrorKind::Io);

        assert_eq!(err.msg(), "panicked while retrying");
    }
}

mod download_chunks {
    use tracing::Span;

    use crate::{
        condow_client::NoLocation,
        config::Config,
        machinery::{download_chunks, DownloadSpanGuard},
        streams::BytesHint,
        test_utils::*,
        InclusiveRange,
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

        let result_stream = download_chunks(
            client.into(),
            NoLocation,
            range,
            bytes_hint,
            config,
            Default::default(),
            DownloadSpanGuard::new(Span::none()),
        )
        .await
        .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_std_range_usize()]);
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

        let result_stream = download_chunks(
            client.into(),
            NoLocation,
            range,
            bytes_hint,
            config,
            Default::default(),
            DownloadSpanGuard::new(Span::none()),
        )
        .await
        .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_std_range_usize()]);
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

        let result_stream = download_chunks(
            client.into(),
            NoLocation,
            range,
            bytes_hint,
            config,
            Default::default(),
            DownloadSpanGuard::new(Span::none()),
        )
        .await
        .unwrap();

        let result = result_stream.into_vec().await.unwrap();

        assert_eq!(&result, &data[range.to_std_range_usize()]);
    }
}
