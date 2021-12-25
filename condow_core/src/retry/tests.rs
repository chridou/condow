use super::*;
use crate::errors::CondowErrorKind;

const RETRYABLE: CondowErrorKind = CondowErrorKind::Remote;
const ANOTHER_RETRYABLE: CondowErrorKind = CondowErrorKind::Io;
const NON_RETRYABLE: CondowErrorKind = CondowErrorKind::NotFound;

const ERROR_KINDS: [CondowErrorKind; 2] = [NON_RETRYABLE, RETRYABLE];

#[test]
fn check_error_kinds() {
    // Check that we have retryable and non retryable errors
    // These are the assumptions made for the tests.
    assert!(RETRYABLE.is_retryable(), "RETRYABLE is not retryable!");
    assert!(
        ANOTHER_RETRYABLE.is_retryable(),
        "ANOTHER_RETRYABLE is not retryable!"
    );
    assert!(!NON_RETRYABLE.is_retryable(), "NON_RETRYABLE is retryable!");
}

mod retry_download_get_stream {
    //! Tests for the function `retry_download_get_stream`
    //!
    //! Tests are performed by using a function `run_get_stream`.
    //!
    //! This function returns an `Ok` with the number of retries that
    //! have been attempted to achieve success. In case of a failure the
    //! function returns a tuple containing the final error and
    //! the number of retries that were made.
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use futures::{stream, FutureExt};

    use crate::{errors::CondowErrorKind, test_utils::NoLocation};

    use super::*;

    #[tokio::test]
    async fn ok() {
        let errors = vec![];
        assert_eq!(run_get_stream(errors, 0).await, Ok(0));
    }

    #[tokio::test]
    async fn same_errors_1_error_0_retries() {
        let n_errors = 1;
        let n_retries = 0;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_stream(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Err((0, kind))
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_1_error_1_retry() {
        let n_errors = 1;
        let n_retries = 1;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_stream(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Ok(1)
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_2_errors_1_retry() {
        let n_errors = 2;
        let n_retries = 1;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_stream(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Err((1, kind))
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_2_errors_2_retries() {
        let n_errors = 2;
        let n_retries = 2;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_stream(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Ok(2)
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_3_errors_2_retries() {
        let n_errors = 3;
        let n_retries = 2;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_stream(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Err((2, kind))
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn non_retryable_aborts() {
        let n_retries = 2;

        let errors = vec![NON_RETRYABLE];
        let result = run_get_stream(errors, n_retries).await;
        assert_eq!(result, Err((0, NON_RETRYABLE)));
    }

    #[tokio::test]
    async fn non_retryable_aborts_after_a_retryable() {
        let n_retries = 2;

        let errors = vec![RETRYABLE, NON_RETRYABLE];
        let result = run_get_stream(errors, n_retries).await;
        assert_eq!(result, Err((1, NON_RETRYABLE)));
    }

    #[tokio::test]
    async fn last_retryable_is_returned() {
        let n_retries = 1;

        let errors = vec![RETRYABLE, ANOTHER_RETRYABLE];
        let result = run_get_stream(errors, n_retries).await;
        assert_eq!(result, Err((1, ANOTHER_RETRYABLE)));
    }

    #[tokio::test]
    async fn success_after_two_retryables_with_2_retries() {
        let n_retries = 2;

        let errors = vec![RETRYABLE, ANOTHER_RETRYABLE];
        let result = run_get_stream(errors, n_retries).await;
        assert_eq!(result, Ok(2));
    }

    /// Simulates a call to a client
    ///
    /// `fails` are the errors to be returned before a success is delivered
    /// `n_retries` is the number of retries to be attempted after a failure
    ///
    /// Always returns the number of attempted retries and in case of a final error
    /// the kind of the error.
    async fn run_get_stream(
        mut fails: Vec<CondowErrorKind>,
        n_retries: usize,
    ) -> Result<usize, (usize, CondowErrorKind)> {
        #[derive(Clone)]
        struct Client {
            fails_reversed: Arc<Mutex<Vec<CondowErrorKind>>>,
        }

        impl CondowClient for Client {
            type Location = NoLocation;

            fn get_size(
                &self,
                _location: Self::Location,
            ) -> futures::future::BoxFuture<'static, Result<u64, CondowError>> {
                unimplemented!()
            }

            fn download(
                &self,
                _location: Self::Location,
                _spec: DownloadSpec,
            ) -> futures::future::BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>
            {
                let mut fails = self.fails_reversed.lock().unwrap();

                if fails.is_empty() {
                    let stream = Box::pin(stream::empty()) as BytesStream;
                    futures::future::ready(Ok((stream, BytesHint::new_no_hint()))).boxed()
                } else {
                    let err = CondowError::from(fails.pop().unwrap());
                    futures::future::ready(Err(err)).boxed()
                }
            }
        }

        fails.reverse();
        let client = Client {
            fails_reversed: Arc::new(Mutex::new(fails)),
        };

        #[derive(Clone)]
        struct Probe(Arc<AtomicUsize>);

        impl Reporter for Probe {
            fn retry(
                &self,
                _location: &dyn std::fmt::Display,
                _error: &CondowError,
                _next_in: Duration,
            ) {
                // Count the number of retries
                self.0.as_ref().fetch_add(1, Ordering::SeqCst);
            }
        }

        let config = RetryConfig::default()
            .max_attempts(n_retries)
            .max_delay_ms(0);

        let probe = Probe(Default::default());
        match retry_download_get_stream(
            &client,
            NoLocation,
            DownloadSpec::Complete,
            &config,
            &probe,
        )
        .await
        {
            Ok(_) => Ok(probe.0.load(Ordering::SeqCst)),
            Err(err) => Err((probe.0.load(Ordering::SeqCst), err.kind())),
        }
    }
}

mod retry_get_size {
    //! Tests for the function `retry_get_size`
    //!
    //! Tests are performed by using a function `run_get_size`.
    //!
    //! This function returns an `Ok` with the number of retries that
    //! have been attempted to achieve success. In case of a failure the
    //! function returns a tuple containing the final error and
    //! the number of retries that were made.
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use futures::FutureExt;

    use crate::{errors::CondowErrorKind, test_utils::NoLocation};

    use super::*;

    #[tokio::test]
    async fn ok() {
        let errors = vec![];
        assert_eq!(run_get_size(errors, 0).await, Ok(0));
    }

    #[tokio::test]
    async fn same_errors_1_error_0_retries() {
        let n_errors = 1;
        let n_retries = 0;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_size(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Err((0, kind))
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_1_error_1_retry() {
        let n_errors = 1;
        let n_retries = 1;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_size(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Ok(1)
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_2_errors_1_retry() {
        let n_errors = 2;
        let n_retries = 1;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_size(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Err((1, kind))
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_2_errors_2_retries() {
        let n_errors = 2;
        let n_retries = 2;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_size(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Ok(2)
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn same_errors_3_errors_2_retries() {
        let n_errors = 3;
        let n_retries = 2;

        for kind in ERROR_KINDS {
            let errors = vec![kind; n_errors];
            let result = run_get_size(errors, n_retries).await;
            let expected = if kind.is_retryable() {
                Err((2, kind))
            } else {
                Err((0, kind))
            };

            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn non_retryable_aborts() {
        let n_retries = 2;

        let errors = vec![NON_RETRYABLE];
        let result = run_get_size(errors, n_retries).await;
        assert_eq!(result, Err((0, NON_RETRYABLE)));
    }

    #[tokio::test]
    async fn non_retryable_aborts_after_a_retryable() {
        let n_retries = 2;

        let errors = vec![RETRYABLE, NON_RETRYABLE];
        let result = run_get_size(errors, n_retries).await;
        assert_eq!(result, Err((1, NON_RETRYABLE)));
    }

    #[tokio::test]
    async fn last_retryable_is_returned() {
        let n_retries = 1;

        let errors = vec![RETRYABLE, ANOTHER_RETRYABLE];
        let result = run_get_size(errors, n_retries).await;
        assert_eq!(result, Err((1, ANOTHER_RETRYABLE)));
    }

    #[tokio::test]
    async fn success_after_two_retryables_with_2_retries() {
        let n_retries = 2;

        let errors = vec![RETRYABLE, ANOTHER_RETRYABLE];
        let result = run_get_size(errors, n_retries).await;
        assert_eq!(result, Ok(2));
    }

    /// Simulates a call to a client
    ///
    /// `fails` are the errors to be returned before a success is delivered
    /// `n_retries` is the number of retries to be attempted after a failure
    ///
    /// Always returns the number of attempted retries and in case of a final error
    /// the kind of the error.
    async fn run_get_size(
        mut fails: Vec<CondowErrorKind>,
        n_retries: usize,
    ) -> Result<usize, (usize, CondowErrorKind)> {
        #[derive(Clone)]
        struct Client {
            fails_reversed: Arc<Mutex<Vec<CondowErrorKind>>>,
        }

        impl CondowClient for Client {
            type Location = NoLocation;

            fn get_size(
                &self,
                _location: Self::Location,
            ) -> futures::future::BoxFuture<'static, Result<u64, CondowError>> {
                let mut fails = self.fails_reversed.lock().unwrap();

                if fails.is_empty() {
                    futures::future::ready(Ok(0)).boxed()
                } else {
                    let err = CondowError::from(fails.pop().unwrap());
                    futures::future::ready(Err(err)).boxed()
                }
            }

            fn download(
                &self,
                _location: Self::Location,
                _spec: DownloadSpec,
            ) -> futures::future::BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>
            {
                unimplemented!()
            }
        }

        fails.reverse();
        let client = Client {
            fails_reversed: Arc::new(Mutex::new(fails)),
        };

        #[derive(Clone)]
        struct Probe(Arc<AtomicUsize>);

        impl Reporter for Probe {
            fn retry(
                &self,
                _location: &dyn std::fmt::Display,
                _error: &CondowError,
                _next_in: Duration,
            ) {
                // Count the number of retries
                self.0.as_ref().fetch_add(1, Ordering::SeqCst);
            }
        }

        let config = RetryConfig::default()
            .max_attempts(n_retries)
            .max_delay_ms(0);

        let probe = Probe(Default::default());
        match retry_get_size(&client, NoLocation, &config, &probe).await {
            Ok(_) => Ok(probe.0.load(Ordering::SeqCst)),
            Err(err) => Err((probe.0.load(Ordering::SeqCst), err.kind())),
        }
    }
}
mod iterator {
    //! This module tests the iterator logic itself.
    //! It makes no assumptions on whether a configuration from which
    //! an iterator is created makes sense from a users perspective or is even
    //! valid. Just make sure the implementation is "sound".
    use super::*;

    #[test]
    fn empty() {
        let mut iter = RetryConfig::default().max_attempts(0).iterator();

        assert!(iter.next().is_none());
    }

    #[test]
    fn one() {
        let mut iter = RetryConfig::default()
            .max_attempts(1)
            .initial_delay_ms(100)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn two_factor_default() {
        let mut iter = RetryConfig::default()
            .max_attempts(2)
            .initial_delay_ms(100)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(150)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn three_factor_default() {
        let mut iter = RetryConfig::default()
            .max_attempts(3)
            .initial_delay_ms(100)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(150)));
        assert_eq!(iter.next(), Some(Duration::from_millis(225)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn three_factor_one() {
        let mut iter = RetryConfig::default()
            .max_attempts(3)
            .delay_factor(1)
            .initial_delay_ms(100)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn three_factor_half() {
        // Valid behaviour of the iterator even though
        // an invalid configuration
        let mut iter = RetryConfig::default()
            .max_attempts(3)
            .delay_factor(0.5)
            .initial_delay_ms(400)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(400)));
        assert_eq!(iter.next(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn three_factor_zero() {
        // Valid behaviour of the iterator even though
        // an invalid configuration
        let mut iter = RetryConfig::default()
            .max_attempts(3)
            .delay_factor(0.0)
            .initial_delay_ms(100)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn initial_zero_three_factor_zero() {
        // Valid behaviour of the iterator even though
        // an invalid configuration
        let mut iter = RetryConfig::default()
            .max_attempts(3)
            .delay_factor(0.0)
            .initial_delay_ms(0)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn initial_zero_three_factor_2() {
        // Valid behaviour of the iterator and a valid configuration
        // even though it doesn't make too much sense
        let mut iter = RetryConfig::default()
            .max_attempts(3)
            .delay_factor(2.0)
            .initial_delay_ms(0)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert_eq!(iter.next(), Some(Duration::from_millis(0)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn five_max_delay() {
        let mut iter = RetryConfig::default()
            .max_attempts(5)
            .delay_factor(2.0)
            .initial_delay_ms(200)
            .max_delay_ms(800)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next(), Some(Duration::from_millis(400)));
        assert_eq!(iter.next(), Some(Duration::from_millis(800)));
        assert_eq!(iter.next(), Some(Duration::from_millis(800)));
        assert_eq!(iter.next(), Some(Duration::from_millis(800)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn five_max_delay_initial_is_covered() {
        let mut iter = RetryConfig::default()
            .max_attempts(5)
            .delay_factor(2.0)
            .initial_delay_ms(200)
            .max_delay_ms(100)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert!(iter.next().is_none());
    }

    #[test]
    fn five_max_delay_initial_is_greater_than_max_but_factor_is_half() {
        // Valid behaviour of the iterator
        // even though it doesn't make too much sense
        // from a users perspective
        let mut iter = RetryConfig::default()
            .max_attempts(5)
            .delay_factor(0.5)
            .initial_delay_ms(800)
            .max_delay_ms(200)
            .iterator();

        assert_eq!(iter.next(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next(), Some(Duration::from_millis(200)));
        assert_eq!(iter.next(), Some(Duration::from_millis(100)));
        assert_eq!(iter.next(), Some(Duration::from_millis(50)));
        assert!(iter.next().is_none());
    }
}
