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
                        .max_buffers_full_delay_ms(0)
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
                        .max_buffers_full_delay_ms(0)
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
                    ..TestCondowClient::default()
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 2, 3, 4, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .max_buffers_full_delay_ms(0)
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
                                (),
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
                    ..TestCondowClient::default()
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 2, 3, 4, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .max_buffers_full_delay_ms(0)
                            .part_size_bytes(part_size)
                            .always_get_size(false) // case to test
                            .max_concurrency(n_concurrency);
                        let condow = Condow::new(client.clone(), config).unwrap();

                        for from_idx in [0u64, 101, 255, 256] {
                            let range = from_idx..;

                            let result_stream = condow
                                .blob()
                                .range(range)
                                .download_chunks_unordered()
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
                    ..TestCondowClient::default()
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 2, 3, 4, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .max_buffers_full_delay_ms(0)
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
                                (),
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
                    ..TestCondowClient::default()
                };

                for part_size in [1u64, 3, 50, 1_000] {
                    for n_concurrency in [1usize, 2, 3, 4, 10] {
                        let config = Config::default()
                            .buffer_size(buffer_size)
                            .max_buffers_full_delay_ms(0)
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
                                (),
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
                            ..TestCondowClient::default()
                        };

                        for part_size in [1u64, 3, 50, 1_000] {
                            for n_concurrency in [1usize, 2, 3, 4, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .max_buffers_full_delay_ms(0)
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
                                        (),
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
                            ..TestCondowClient::default()
                        };

                        for part_size in [1u64, 3, 50, 1_000] {
                            for n_concurrency in [1usize, 2, 3, 4, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .max_buffers_full_delay_ms(0)
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
                                        (),
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
                            ..TestCondowClient::default()
                        };

                        for part_size in [1u64, 33] {
                            for n_concurrency in [1usize, 2, 3, 4, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .max_buffers_full_delay_ms(0)
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
                                            (),
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
                            ..TestCondowClient::default()
                        };

                        for part_size in [1u64, 33] {
                            for n_concurrency in [1usize, 2, 3, 4, 10] {
                                let config = Config::default()
                                    .buffer_size(buffer_size)
                                    .max_buffers_full_delay_ms(0)
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
                                            (),
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

mod probe_events {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use crate::{
        config::{Config, SequentialDownloadMode},
        probe::Probe,
        test_utils::TestCondowClient,
        Condow,
    };

    #[tokio::test]
    async fn test_open_close() {
        //! We test different levels of concurrency since there are special
        //! implementations and each of them has to trifgger the events.

        let n_concurencies = [1usize, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20];

        for n_concurrency in n_concurencies {
            let client = TestCondowClient::new().max_chunk_size(10);
            let config = Config::default()
                .part_size_bytes(5)
                .sequential_download_mode(SequentialDownloadMode::KeepParts)
                .max_concurrency(n_concurrency);
            let condow = Condow::new(client.clone(), config).unwrap();

            let probe = TestProbe::new();
            let result = condow
                .blob()
                .range(0..100)
                .probe(Arc::new(probe.clone()))
                .download_into_vec()
                .await
                .unwrap();

            assert_eq!(result, client.data_slice()[0..100]);

            assert_eq!(
                probe.downloads_open(),
                0,
                "downloads open - n_conc: {n_concurrency}"
            );
            assert_eq!(
                probe.downloads_started(),
                1,
                "downloads started - n_conc: {n_concurrency}"
            );
            assert_eq!(
                probe.parts_open(),
                0,
                "parts open - n_conc: {n_concurrency}"
            );
            assert_eq!(
                probe.parts_received(),
                20,
                "parts received - n_conc: {n_concurrency}"
            );
        }
    }

    #[derive(Clone)]
    struct TestProbe {
        downloads_open: Arc<AtomicUsize>,
        downloads_started: Arc<AtomicUsize>,
        parts_open: Arc<AtomicUsize>,
        parts_received: Arc<AtomicUsize>,
    }

    impl TestProbe {
        fn new() -> Self {
            Self {
                downloads_open: Default::default(),
                downloads_started: Default::default(),
                parts_open: Default::default(),
                parts_received: Default::default(),
            }
        }

        fn downloads_open(&self) -> usize {
            self.downloads_open.load(Ordering::SeqCst)
        }
        fn downloads_started(&self) -> usize {
            self.downloads_started.load(Ordering::SeqCst)
        }
        fn parts_open(&self) -> usize {
            self.parts_open.load(Ordering::SeqCst)
        }
        fn parts_received(&self) -> usize {
            self.parts_received.load(Ordering::SeqCst)
        }
    }

    impl Probe for TestProbe {
        fn download_started(&self) {
            self.downloads_open.fetch_add(1, Ordering::SeqCst);
            self.downloads_started.fetch_add(1, Ordering::SeqCst);
        }
        fn download_completed(&self, _time: std::time::Duration) {
            self.downloads_open.fetch_sub(1, Ordering::SeqCst);
        }
        fn download_failed(&self, _time: Option<std::time::Duration>) {
            self.downloads_open.fetch_sub(1, Ordering::SeqCst);
        }

        fn part_started(&self, _part_index: u64, _range: crate::InclusiveRange) {
            self.parts_open.fetch_add(1, Ordering::SeqCst);
            self.parts_received.fetch_add(1, Ordering::SeqCst);
        }
        fn part_completed(
            &self,
            _part_index: u64,
            _n_chunks: usize,
            _n_bytes: u64,
            _time: std::time::Duration,
        ) {
            self.parts_open.fetch_sub(1, Ordering::SeqCst);
        }
        fn part_failed(
            &self,
            _error: &crate::errors::CondowError,
            _part_index: u64,
            _range: &crate::InclusiveRange,
        ) {
            self.parts_open.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
