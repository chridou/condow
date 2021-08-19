mod file {
    use std::sync::Arc;

    use crate::reporter::SimpleReporterFactory;
    use crate::{config::Config, test_utils::*, Condow};

    use crate::test_utils::create_test_data;

    #[tokio::test]
    async fn download_blob() {
        let buffer_size = 10;

        let data = Arc::new(create_test_data());

        for chunk_size in [1, 3, 5] {
            let client = TestCondowClient {
                data: Arc::clone(&data),
                max_jitter_ms: 0,
                include_size_hint: true,
                max_chunk_size: chunk_size,
            };

            for part_size in [1usize, 3, 50, 1_000] {
                for n_concurrency in [1usize, 10] {
                    let config = Config::default()
                        .buffer_size(buffer_size)
                        .buffers_full_delay_ms(0)
                        .part_size_bytes(part_size)
                        .max_concurrency(n_concurrency);
                    let condow =
                        Condow::new_with_reporting(client.clone(), config, SimpleReporterFactory)
                            .unwrap();

                    let result_stream = condow
                        .download_chunks_internal((), .., Default::default())
                        .await
                        .unwrap();

                    let result = result_stream.into_stream().into_vec().await.unwrap();

                    assert_eq!(&result, data.as_ref());
                }
            }
        }
    }
}

mod range {
    mod open {
        use std::sync::Arc;

        use crate::reporter::SimpleReporterFactory;
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

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new_with_reporting(
                            client.clone(),
                            config,
                            SimpleReporterFactory,
                        )
                        .unwrap();

                        for from_idx in [0usize, 101, 255, 256] {
                            let range = from_idx..;

                            let result_stream = condow
                                .download_chunks_internal(
                                    (),
                                    range.clone(),
                                    crate::GetSizeMode::Always,
                                )
                                .await
                                .unwrap();

                            let result = result_stream.into_stream().into_vec().await.unwrap();

                            let check_range = from_idx.min(data.len())..;
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

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new_with_reporting(
                            client.clone(),
                            config,
                            SimpleReporterFactory,
                        )
                        .unwrap();

                        for from_idx in [0usize, 101, 255, 256] {
                            let range = from_idx..;

                            let result_stream = condow
                                .download_chunks_internal(
                                    (),
                                    range.clone(),
                                    crate::GetSizeMode::Required,
                                )
                                .await
                                .unwrap();

                            let result = result_stream.into_stream().into_vec().await.unwrap();

                            let check_range = from_idx.min(data.len())..;
                            assert_eq!(&result, &data[check_range]);
                        }
                    }
                }
            }
        }
    }

    mod closed {
        use std::sync::Arc;

        use crate::reporter::SimpleReporterFactory;
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

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new_with_reporting(
                            client.clone(),
                            config,
                            SimpleReporterFactory,
                        )
                        .unwrap();

                        for end_incl in [0usize, 2, 101, 255] {
                            let range = 0..=end_incl;
                            let expected_range_end = (end_incl + 1).min(data.len());

                            let result_stream = condow
                                .download_chunks_internal(
                                    (),
                                    range.clone(),
                                    crate::GetSizeMode::Default,
                                )
                                .await
                                .unwrap();

                            let result = result_stream.into_stream().into_vec().await.unwrap();

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

                for part_size in [1usize, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new_with_reporting(
                            client.clone(),
                            config,
                            SimpleReporterFactory,
                        )
                        .unwrap();

                        for end_excl in [0usize, 2, 101, 255, 256] {
                            let range = 0..end_excl;
                            let expected_range_end = end_excl.min(data.len());

                            let result_stream = condow
                                .download_chunks_internal(
                                    (),
                                    range.clone(),
                                    crate::GetSizeMode::Default,
                                )
                                .await
                                .unwrap();

                            let result = result_stream.into_stream().into_vec().await.unwrap();

                            assert_eq!(&result, &data[0..expected_range_end]);
                        }
                    }
                }
            }
        }

        mod from_to {
            mod start_at_0 {
                use std::sync::Arc;

                use crate::reporter::SimpleReporterFactory;
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

                        for part_size in [1usize, 3, 50, 1_000] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new_with_reporting(
                                    client.clone(),
                                    config,
                                    SimpleReporterFactory,
                                )
                                .unwrap();

                                for end_incl in [0usize, 2, 101, 255, 255] {
                                    let range = 0..=end_incl;
                                    let expected_range_end = (end_incl + 1).min(data.len());

                                    let result_stream = condow
                                        .download_chunks_internal(
                                            (),
                                            range,
                                            crate::GetSizeMode::Default,
                                        )
                                        .await
                                        .unwrap();

                                    let result =
                                        result_stream.into_stream().into_vec().await.unwrap();

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

                        for part_size in [1usize, 3, 50, 1_000] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new_with_reporting(
                                    client.clone(),
                                    config,
                                    SimpleReporterFactory,
                                )
                                .unwrap();

                                for end_excl in [0usize, 2, 101, 255, 256] {
                                    let range = 0..end_excl;
                                    let expected_range_end = end_excl.min(data.len());

                                    let result_stream = condow
                                        .download_chunks_internal(
                                            (),
                                            range,
                                            crate::GetSizeMode::Default,
                                        )
                                        .await
                                        .unwrap();

                                    let result =
                                        result_stream.into_stream().into_vec().await.unwrap();

                                    assert_eq!(&result, &data[0..expected_range_end]);
                                }
                            }
                        }
                    }
                }
            }

            mod start_after_0 {
                use std::sync::Arc;

                use crate::reporter::SimpleReporterFactory;
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

                        for part_size in [1usize, 33] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new_with_reporting(
                                    client.clone(),
                                    config,
                                    SimpleReporterFactory,
                                )
                                .unwrap();

                                for start in [1usize, 87, 101, 201] {
                                    for len in [1, 10, 100] {
                                        let end_incl = start + len;
                                        let range = start..=(end_incl);
                                        let expected_range_end = (end_incl + 1).min(data.len());

                                        let result_stream = condow
                                            .download_chunks_internal(
                                                (),
                                                range,
                                                crate::GetSizeMode::Default,
                                            )
                                            .await
                                            .unwrap();

                                        let result =
                                            result_stream.into_stream().into_vec().await.unwrap();

                                        assert_eq!(&result, &data[start..expected_range_end]);
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

                        for part_size in [1usize, 33] {
                            for n_concurrency in [1usize, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .buffers_full_delay_ms(0)
                                    .part_size_bytes(part_size)
                                    .max_concurrency(n_concurrency);
                                let condow = Condow::new_with_reporting(
                                    client.clone(),
                                    config,
                                    SimpleReporterFactory,
                                )
                                .unwrap();

                                for start in [1usize, 87, 101, 201] {
                                    for len in [1, 10, 100] {
                                        let end_excl = start + len;
                                        let range = start..(end_excl);
                                        let expected_range_end = end_excl.min(data.len());

                                        let result_stream = condow
                                            .download_chunks_internal(
                                                (),
                                                range,
                                                crate::GetSizeMode::Default,
                                            )
                                            .await
                                            .unwrap();

                                        let result =
                                            result_stream.into_stream().into_vec().await.unwrap();

                                        assert_eq!(&result, &data[start..expected_range_end]);
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
