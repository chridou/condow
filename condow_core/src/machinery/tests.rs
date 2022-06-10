mod download_chunks {
    use crate::{
        condow_client::{failing_client_simulator::FailingClientSimulatorBuilder, IgnoreLocation},
        config::{Config, PartSizeBytes, SequentialDownloadMode},
        errors::CondowErrorKind,
        machinery::download_chunks,
        test_utils::TestCondowClient,
        InclusiveRange,
    };

    const CONCURRENCIES: &[usize] = &[1, 2, 3, 4, 5, 10];
    const SEQ_MODES: &[SequentialDownloadMode] = &[
        SequentialDownloadMode::KeepParts,
        SequentialDownloadMode::MergeParts,
        SequentialDownloadMode::Repartition {
            part_size: PartSizeBytes::from_u64(3),
        },
    ];

    #[tokio::test]
    async fn download_ok() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_chunks(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let result_bytes = stream.into_vec().await.unwrap();

                assert_eq!(result_bytes, blob);
            }
        }
    }

    #[tokio::test]
    async fn download_request_failure() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .failure(CondowErrorKind::Other)
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_chunks(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                assert_eq!(
                    stream.into_vec().await.unwrap_err().kind(),
                    CondowErrorKind::Other
                );
            }
        }
    }

    #[tokio::test]
    async fn download_stream_error() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .success_with_stream_failure(5)
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_chunks(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                assert_eq!(
                    stream.into_vec().await.unwrap_err().kind(),
                    CondowErrorKind::Io
                );
            }
        }
    }

    #[tokio::test]
    async fn download_panic_no_retries() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .panic("BAMM!")
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_chunks(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let err = stream.into_vec().await.unwrap_err();

                assert_eq!(err.kind(), CondowErrorKind::Other);

                assert_eq!(err.msg(), "download ended unexpectedly due to a panic");
            }
        }
    }

    #[tokio::test]
    async fn download_panic_in_retry_after_stream_failure() {
        let blob = (0u8..100).collect::<Vec<_>>();
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
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

                let result = download_chunks(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let _err = stream.into_vec().await.unwrap_err();

                // assert_eq!(err.kind(), CondowErrorKind::Io); TODO: Check!

                // assert_eq!(err.msg(), "panicked while retrying");
            }
        }
    }

    #[tokio::test]
    async fn download_panic_while_streaming() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .success_with_stream_panic(5)
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_chunks(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let err = stream.into_vec().await.unwrap_err();

                assert_eq!(err.kind(), CondowErrorKind::Other);

                assert_eq!(err.msg(), "download ended unexpectedly due to a panic");
            }
        }
    }

    #[tokio::test]
    async fn download_panic_in_retry_while_streaming() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
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

                let result = download_chunks(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let _err = stream.into_vec().await.unwrap_err();

                // assert_eq!(err.kind(), CondowErrorKind::Io); TODO: Check

                //assert_eq!(err.msg(), "panicked while retrying");
            }
        }
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_smaller_than_part_size() {
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let buffer_size = 10;
                let client = TestCondowClient::new().max_chunk_size(3);
                let data = client.data();

                let config = Config::default()
                    .buffer_size(buffer_size)
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode);

                let range = InclusiveRange(0, 8);

                let result_stream =
                    download_chunks(client.into(), config, IgnoreLocation, range, (), None)
                        .await
                        .unwrap();

                let result = result_stream.into_vec().await.unwrap();

                assert_eq!(&result, &data[range.to_std_range_usize()]);
            }
        }
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_equal_size_as_part_size() {
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let buffer_size = 10;
                let client = TestCondowClient::new().max_chunk_size(3);
                let data = client.data();

                let config = Config::default()
                    .buffer_size(buffer_size)
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode);

                let range = InclusiveRange(0, 9);

                let result_stream =
                    download_chunks(client.into(), config, IgnoreLocation, range, (), None)
                        .await
                        .unwrap();

                let result = result_stream.into_vec().await.unwrap();

                assert_eq!(&result, &data[range.to_std_range_usize()]);
            }
        }
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_larger_than_part_size() {
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let buffer_size = 10;
                let client = TestCondowClient::new().max_chunk_size(3);
                let data = client.data();

                let config = Config::default()
                    .buffer_size(buffer_size)
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode);

                let range = InclusiveRange(0, 10);

                let result_stream =
                    download_chunks(client.into(), config, IgnoreLocation, range, (), None)
                        .await
                        .unwrap();

                let result = result_stream.into_vec().await.unwrap();

                assert_eq!(&result, &data[range.to_std_range_usize()]);
            }
        }
    }
}

mod download_bytes {
    use crate::{
        condow_client::{failing_client_simulator::FailingClientSimulatorBuilder, IgnoreLocation},
        config::{Config, PartSizeBytes, SequentialDownloadMode},
        errors::CondowErrorKind,
        machinery::download_bytes,
        test_utils::TestCondowClient,
        InclusiveRange,
    };

    const CONCURRENCIES: &[usize] = &[1, 2, 3, 4, 5, 10];
    const SEQ_MODES: &[SequentialDownloadMode] = &[
        SequentialDownloadMode::KeepParts,
        SequentialDownloadMode::MergeParts,
        SequentialDownloadMode::Repartition {
            part_size: PartSizeBytes::from_u64(3),
        },
    ];

    #[tokio::test]
    async fn download_ok() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_bytes(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let result_bytes = stream.into_vec().await.unwrap();

                assert_eq!(result_bytes, blob);
            }
        }
    }

    #[tokio::test]
    async fn download_request_failure() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .failure(CondowErrorKind::Other)
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_bytes(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                assert_eq!(
                    stream.into_vec().await.unwrap_err().kind(),
                    CondowErrorKind::Other
                );
            }
        }
    }

    #[tokio::test]
    async fn download_stream_error() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .success_with_stream_failure(5)
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_bytes(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                assert_eq!(
                    stream.into_vec().await.unwrap_err().kind(),
                    CondowErrorKind::Io
                );
            }
        }
    }

    #[tokio::test]
    async fn download_panic_no_retries() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .panic("BAMM!")
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_bytes(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let err = stream.into_vec().await.unwrap_err();

                assert_eq!(err.kind(), CondowErrorKind::Other);

                // assert_eq!(err.msg(), "download ended unexpectedly due to a panic");
            }
        }
    }

    #[tokio::test]
    async fn download_panic_in_retry_after_stream_failure() {
        let blob = (0u8..100).collect::<Vec<_>>();
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
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

                let result = download_bytes(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let _err = stream.into_vec().await.unwrap_err();

                // assert_eq!(err.kind(), CondowErrorKind::Io); TODO: Check!

                // assert_eq!(err.msg(), "panicked while retrying");
            }
        }
    }

    #[tokio::test]
    async fn download_panic_while_streaming() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
                    .disable_retries();

                let condow = FailingClientSimulatorBuilder::default()
                    .blob(blob.clone())
                    .responses()
                    .success_with_stream_panic(5)
                    .finish()
                    .condow(config)
                    .unwrap();

                let result = download_bytes(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let err = stream.into_vec().await.unwrap_err();

                assert_eq!(err.kind(), CondowErrorKind::Other);

                assert_eq!(err.msg(), "download ended unexpectedly due to a panic");
            }
        }
    }

    #[tokio::test]
    async fn download_panic_in_retry_while_streaming() {
        let blob = (0u8..100).collect::<Vec<_>>();

        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let config = Config::default()
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode)
                    .ensure_active_pull(true)
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

                let result = download_bytes(
                    condow.retry_client(),
                    condow.config,
                    IgnoreLocation,
                    0..100,
                    (),
                    None,
                )
                .await;

                let stream = result.unwrap();

                let _err = stream.into_vec().await.unwrap_err();

                // assert_eq!(err.kind(), CondowErrorKind::Io); TODO: Check

                //assert_eq!(err.msg(), "panicked while retrying");
            }
        }
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_smaller_than_part_size() {
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let buffer_size = 10;
                let client = TestCondowClient::new().max_chunk_size(3);
                let data = client.data();

                let config = Config::default()
                    .buffer_size(buffer_size)
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode);

                let range = InclusiveRange(0, 8);

                let result_stream =
                    download_bytes(client.into(), config, IgnoreLocation, range, (), None)
                        .await
                        .unwrap();

                let result = result_stream.into_vec().await.unwrap();

                assert_eq!(&result, &data[range.to_std_range_usize()]);
            }
        }
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_equal_size_as_part_size() {
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let buffer_size = 10;
                let client = TestCondowClient::new().max_chunk_size(3);
                let data = client.data();

                let config = Config::default()
                    .buffer_size(buffer_size)
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode);

                let range = InclusiveRange(0, 9);

                let result_stream =
                    download_bytes(client.into(), config, IgnoreLocation, range, (), None)
                        .await
                        .unwrap();

                let result = result_stream.into_vec().await.unwrap();

                assert_eq!(&result, &data[range.to_std_range_usize()]);
            }
        }
    }

    #[tokio::test]
    async fn from_0_to_inclusive_range_larger_than_part_size() {
        for &n_concurrency in CONCURRENCIES {
            for &seq_mode in SEQ_MODES {
                let buffer_size = 10;
                let client = TestCondowClient::new().max_chunk_size(3);
                let data = client.data();

                let config = Config::default()
                    .buffer_size(buffer_size)
                    .max_buffers_full_delay_ms(0)
                    .part_size_bytes(10)
                    .max_concurrency(n_concurrency)
                    .sequential_download_mode(seq_mode);

                let range = InclusiveRange(0, 10);

                let result_stream =
                    download_bytes(client.into(), config, IgnoreLocation, range, (), None)
                        .await
                        .unwrap();

                let result = result_stream.into_vec().await.unwrap();

                assert_eq!(&result, &data[range.to_std_range_usize()]);
            }
        }
    }
}
