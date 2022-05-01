mod blob {
    /*
    use std::sync::Arc;
    use std::time::Duration;

    use crate::condow_client::NoLocation;
    use crate::probe::SimpleReporterFactory;
    use crate::{config::Config, test_utils::*, Condow};

    use crate::test_utils::create_test_data;

    #[tokio::test]
    async fn download_complete_blob_with_reporter() {
        let buffer_size = 10;

        let data = Arc::new(create_test_data());

        for chunk_size in [1, 3, 5] {
            let client = TestCondowClient {
                data: Arc::clone(&data),
                max_jitter_ms: 0,
                include_size_hint: true,
                max_chunk_size: chunk_size,
            };

            for part_size in [1u64, 3, 50, 1_000] {
                for n_concurrency in [1usize, 10] {
                    let config = Config::default()
                        .buffer_size(buffer_size)
                        .buffers_full_delay_ms(0)
                        .part_size_bytes(part_size)
                        .max_concurrency(n_concurrency);
                    let condow = Condow::new(client.clone(), config).unwrap();

                    let downloader =
                        condow.downloader_with_reporting(SimpleReporterFactory::default());

                    let result_stream = downloader.download_rep(NoLocation, ..).await.unwrap();

                    let result = result_stream.into_stream().into_vec().await.unwrap();

                    assert_eq!(&result, data.as_ref());
                }
            }
        }
    }

    #[tokio::test]
    async fn check_simple_reporter() {
        let buffer_size = 10;

        let data = Arc::new(create_test_data());

        for chunk_size in [1, 3, 5] {
            let client = TestCondowClient {
                data: Arc::clone(&data),
                max_jitter_ms: 0,
                include_size_hint: true,
                max_chunk_size: chunk_size,
            };

            for part_size in [1u64, 3, 50, 1_000] {
                for n_concurrency in [1usize, 10] {
                    let config = Config::default()
                        .buffer_size(buffer_size)
                        .buffers_full_delay_ms(0)
                        .part_size_bytes(part_size)
                        .max_concurrency(n_concurrency);
                    let condow = Condow::new(client.clone(), config).unwrap();

                    let downloader =
                        condow.blob().probe(SimpleReporterFactory::default());

                    let result_stream = downloader.download_rep(NoLocation, ..).await.unwrap();

                    let report = result_stream.reporter.report();
                    assert!(report.download_time > Duration::ZERO);
                }
            }
        }
    }*/
}

mod range {
    mod open {
        use std::sync::Arc;

        use crate::condow_client::IgnoreLocation;
        use crate::machinery;
        use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

        #[tokio::test]
        async fn from_always_get_size() {
            let buffer_size = 10;

            let data = Arc::new(create_test_data());

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 0,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .always_get_size(true) // case to test
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        for from_idx in [0u64, 101, 255, 256] {
                            let range = from_idx..;

                            let result_stream = machinery::download_range(
                                condow.client.clone(),
                                condow.config.clone(),
                                IgnoreLocation,
                                range.clone(),
                                Default::default(),
                            )
                            .await
                            .unwrap();

                            let result = result_stream.into_vec().await.unwrap();

                            let check_range = (from_idx as usize).min(data.len())..;
                            assert_eq!(&result, &data[check_range]);
                        }
                    }
                }
            }
        }

        #[tokio::test]
        async fn from_when_required_get_size() {
            let buffer_size = 10;

            let data = Arc::new(create_test_data());

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 0,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .always_get_size(false) // case to test
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        for from_idx in [0u64, 101, 255, 256] {
                            let range = from_idx..;

                            let result_stream =
                                condow.blob().range(range).download_chunks().await.unwrap();

                            let result = result_stream.into_vec().await.unwrap();

                            let check_range = (from_idx as usize).min(data.len())..;
                            assert_eq!(&result, &data[check_range]);
                        }
                    }
                }
            }
        }
    }

    mod closed {
        use std::sync::Arc;

        use crate::condow_client::IgnoreLocation;
        use crate::machinery;
        use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

        #[tokio::test]
        async fn to_inclusive() {
            let buffer_size = 10;

            let data = Arc::new(create_test_data());

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 0,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        for end_incl in [0u64, 2, 101, 255] {
                            let range = 0..=end_incl;
                            let expected_range_end = (end_incl as usize + 1).min(data.len());

                            let result_stream = machinery::download_range(
                                condow.client.clone(),
                                condow.config.clone(),
                                IgnoreLocation,
                                range.clone(),
                                Default::default(),
                            )
                            .await
                            .unwrap();

                            let result = result_stream.into_vec().await.unwrap();

                            assert_eq!(&result, &data[0..expected_range_end]);
                        }
                    }
                }
            }
        }

        #[tokio::test]
        async fn to_exclusive() {
            let buffer_size = 10;

            let data = Arc::new(create_test_data());

            for chunk_size in [1, 3, 5] {
                let client = TestCondowClient {
                    data: Arc::clone(&data),
                    max_jitter_ms: 0,
                    include_size_hint: true,
                    max_chunk_size: chunk_size,
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        for end_excl in [0u64, 2, 101, 255, 256] {
                            let range = 0..end_excl;
                            let expected_range_end = (end_excl as usize).min(data.len());

                            let result_stream = machinery::download_range(
                                condow.client.clone(),
                                condow.config.clone(),
                                IgnoreLocation,
                                range.clone(),
                                Default::default(),
                            )
                            .await
                            .unwrap();

                            let result = result_stream.into_vec().await.unwrap();

                            assert_eq!(&result, &data[0..expected_range_end]);
                        }
                    }
                }
            }
        }

        mod from_to {
            mod start_at_0 {
                use std::sync::Arc;

                use crate::condow_client::IgnoreLocation;
                use crate::machinery;
                use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

                #[tokio::test]
                async fn from_0_to_inclusive() {
                    let buffer_size = 10;

                    let data = Arc::new(create_test_data());

                    for chunk_size in [1, 3, 5] {
                        let client = TestCondowClient {
                            data: Arc::clone(&data),
                            max_jitter_ms: 0,
                            include_size_hint: true,
                            max_chunk_size: chunk_size,
                        };

                        for part_size in [1u64, 3, 50, 1_000] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new(client.clone(), config).unwrap();

                                for end_incl in [0u64, 2, 101, 255, 255] {
                                    let range = 0..=end_incl;
                                    let expected_range_end =
                                        (end_incl as usize + 1).min(data.len());

                                    let result_stream = machinery::download_range(
                                        condow.client.clone(),
                                        condow.config.clone(),
                                        IgnoreLocation,
                                        range,
                                        Default::default(),
                                    )
                                    .await
                                    .unwrap();

                                    let result = result_stream.into_vec().await.unwrap();

                                    assert_eq!(&result, &data[0..expected_range_end]);
                                }
                            }
                        }
                    }
                }

                #[tokio::test]
                async fn from_0_to_exclusive() {
                    let buffer_size = 10;

                    let data = Arc::new(create_test_data());

                    for chunk_size in [1, 3, 5] {
                        let client = TestCondowClient {
                            data: Arc::clone(&data),
                            max_jitter_ms: 0,
                            include_size_hint: true,
                            max_chunk_size: chunk_size,
                        };

                        for part_size in [1u64, 3, 50, 1_000] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new(client.clone(), config).unwrap();

                                for end_excl in [0u64, 2, 101, 255, 256] {
                                    let range = 0..end_excl;
                                    let expected_range_end = (end_excl as usize).min(data.len());

                                    let result_stream = machinery::download_range(
                                        condow.client.clone(),
                                        condow.config.clone(),
                                        IgnoreLocation,
                                        range,
                                        Default::default(),
                                    )
                                    .await
                                    .unwrap();

                                    let result = result_stream.into_vec().await.unwrap();

                                    assert_eq!(&result, &data[0..expected_range_end]);
                                }
                            }
                        }
                    }
                }
            }

            mod start_after_0 {
                use std::sync::Arc;

                use crate::condow_client::IgnoreLocation;
                use crate::machinery;
                use crate::{config::Config, test_utils::create_test_data, test_utils::*, Condow};

                #[tokio::test]
                async fn from_to_inclusive() {
                    let buffer_size = 10;

                    let data = Arc::new(create_test_data());

                    for chunk_size in [3] {
                        let client = TestCondowClient {
                            data: Arc::clone(&data),
                            max_jitter_ms: 0,
                            include_size_hint: true,
                            max_chunk_size: chunk_size,
                        };

                        for part_size in [1u64, 33] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new(client.clone(), config).unwrap();

                                for start in [1u64, 87, 101, 201] {
                                    for len in [1, 10, 100] {
                                        let end_incl = start + len;
                                        let range = start..=(end_incl);
                                        let expected_range_end =
                                            (end_incl as usize + 1).min(data.len());

                                        let result_stream = machinery::download_range(
                                            condow.client.clone(),
                                            condow.config.clone(),
                                            IgnoreLocation,
                                            range,
                                            Default::default(),
                                        )
                                        .await
                                        .unwrap();

                                        let result = result_stream.into_vec().await.unwrap();

                                        assert_eq!(
                                            &result,
                                            &data[start as usize..expected_range_end]
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                #[tokio::test]
                async fn from_to_exclusive() {
                    let buffer_size = 10;

                    let data = Arc::new(create_test_data());

                    for chunk_size in [3] {
                        let client = TestCondowClient {
                            data: Arc::clone(&data),
                            max_jitter_ms: 0,
                            include_size_hint: true,
                            max_chunk_size: chunk_size,
                        };

                        for part_size in [1u64, 33] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new(client.clone(), config).unwrap();

                                for start in [1u64, 87, 101, 201] {
                                    for len in [1, 10, 100] {
                                        let end_excl = start + len;
                                        let range = start..(end_excl);
                                        let expected_range_end =
                                            (end_excl as usize).min(data.len());

                                        let result_stream = machinery::download_range(
                                            condow.client.clone(),
                                            condow.config.clone(),
                                            IgnoreLocation,
                                            range,
                                            Default::default(),
                                        )
                                        .await
                                        .unwrap();

                                        let result = result_stream.into_vec().await.unwrap();

                                        assert_eq!(
                                            &result,
                                            &data[start as usize..expected_range_end]
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
