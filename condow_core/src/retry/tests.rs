use crate::errors::CondowErrorKind;

use super::*;

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
    assert!(
        RETRYABLE != ANOTHER_RETRYABLE,
        "retryables must not be the same"
    );
    assert!(!NON_RETRYABLE.is_retryable(), "NON_RETRYABLE is retryable!");
}

mod retry_part_stream {
    //! Tests [RetryPartStream] including retry settings

    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use futures::StreamExt;

    use crate::retry::RetryPartStream;
    use crate::{
        condow_client::{failing_client_simulator::FailingClientSimulatorBuilder, IgnoreLocation},
        config::RetryConfig,
        errors::CondowError,
        probe::Probe,
        retry::tests::{NON_RETRYABLE, RETRYABLE},
        InclusiveRange,
    };

    #[tokio::test]
    async fn complete_no_error() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder().responses().success().never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn success_first_byte() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder().responses().success().never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, n_resumes, 0..=0)
                .await
                .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB[0..=0].to_vec()));
    }

    #[tokio::test]
    async fn err_first_byte() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(0) // bang!
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, n_resumes, 0..=0)
                .await
                .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Err(Vec::new()));
    }

    #[tokio::test]
    async fn success_last_one_byte() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder().responses().success().never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, n_resumes, 15..=15)
                .await
                .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB[15..=15].to_vec()), "BLOB: {:?}", BLOB);
    }

    #[tokio::test]
    async fn err_first_byte_0_resumes() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(0)
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, n_resumes, 0..=0)
                .await
                .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Err(Vec::new()));
    }

    #[tokio::test]
    async fn ok_first_byte_1_resumes() {
        let n_retries = 0;
        let n_resumes = 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(0)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, n_resumes, 0..=0)
                .await
                .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 1, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB[0..=0].to_vec()));
    }

    #[tokio::test]
    async fn err_last_one_byte() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(0) // bang!
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, n_resumes, 15..=15)
                .await
                .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Err(Vec::new()), "BLOB: {:?}", BLOB);
    }

    #[tokio::test]
    async fn err_last_one_byte_1_resume() {
        let n_retries = 0;
        let n_resumes = 1;

        const LAST_BYTE_IDX: u64 = BLOB.len() as u64 - 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(0)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            LAST_BYTE_IDX..=LAST_BYTE_IDX,
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 1, "stream_resume_attempts");
        assert_eq!(
            received,
            Ok(BLOB[LAST_BYTE_IDX as usize..=LAST_BYTE_IDX as usize].to_vec())
        );
    }

    #[tokio::test]
    async fn ok_last_one_byte_2_resumes() {
        let n_retries = 0;
        let n_resumes = 2;

        const LAST_BYTE_IDX: u64 = BLOB.len() as u64 - 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(0)
            .success_with_stream_failure(0)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            LAST_BYTE_IDX..=LAST_BYTE_IDX,
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(
            received,
            Ok(BLOB[LAST_BYTE_IDX as usize..=LAST_BYTE_IDX as usize].to_vec())
        );
    }

    #[tokio::test]
    async fn range_no_error() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder().responses().success().never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            5..=BLOB.len() as u64 - 1,
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB[5..].to_vec()));
    }

    #[tokio::test]
    async fn complete_error_retryable_0_retries() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder().responses().failure(RETRYABLE).never();

        let err = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), RETRYABLE);
    }

    #[tokio::test]
    async fn complete_error_retryable_1_retries() {
        let n_retries = 1;
        let n_resumes = 0;

        let client_builder = get_builder()
            .responses()
            .failure(RETRYABLE)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 1, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_error_non_retryable_1_retries() {
        let n_retries = 1;
        let n_resumes = 0;

        let client_builder = get_builder().responses().failure(NON_RETRYABLE).never();

        let err = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), NON_RETRYABLE);
    }

    #[tokio::test]
    async fn complete_success_broken_stream_0_resumes_0_retries() {
        let n_retries = 0;
        let n_resumes = 0;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..5].to_vec()));
    }

    #[tokio::test]
    async fn complete_success_1_broken_stream_1_resumes_0_retries() {
        let n_retries = 0;
        let n_resumes = 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 1, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_success_2_broken_stream_1_resumes_0_retries() {
        let n_retries = 0;
        let n_resumes = 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .success_with_stream_failure(7)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_success_2_broken_stream_1_resumes_1_retryable_1_retries() {
        let n_retries = 1;
        let n_resumes = 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .failure(RETRYABLE)
            .success_with_stream_failure(7)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 1, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_success_2_broken_stream_1_resumes() {
        let n_retries = 0;
        let n_resumes = 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .success_with_stream_failure(7)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_error_2_broken_stream_1_resumes() {
        let n_retries = 0;
        let n_resumes = 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(7) // orig
            .success_with_stream_failure(0) // no progress, resume 1
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 1, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..7].to_vec()));
    }

    #[tokio::test]
    async fn complete_success_2_broken_stream_2_resumes() {
        let n_retries = 0;
        let n_resumes = 2;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(7) // orig
            .success_with_stream_failure(0) // no progress, resume 1
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_success_2_broken_stream_2_resumes_1_retryable() {
        let n_retries = 1;
        let n_resumes = 2;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(7) // orig
            .success_with_stream_failure(0) // no progress, resume 1
            .failure(RETRYABLE)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 1, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_error_2_broken_stream_1_resumes_1_non_retryable_1_retries() {
        let n_retries = 1;
        let n_resumes = 1;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .failure(NON_RETRYABLE)
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 1, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..5].to_vec()));
    }

    #[tokio::test]
    async fn complete_success_retryable_flips() {
        let n_retries = 1;
        let n_resumes = 3;

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(7)
            .failure(RETRYABLE)
            .success_with_stream_failure(0) // no progress
            .failure(RETRYABLE)
            .success_with_stream_failure(0) // no progress
            .failure(RETRYABLE)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 3, "num_retries");
        assert_eq!(stream_resume_attempts, 3, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn complete_error_retryable_flips() {
        let n_retries = 1;
        let n_resumes = 2; // not enough

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(7)
            .failure(RETRYABLE)
            .success_with_stream_failure(0) // no progress
            .failure(RETRYABLE)
            .success_with_stream_failure(0) // no progress -> bang!
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 2, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..7].to_vec()));
    }

    #[tokio::test]
    async fn complex() {
        let n_retries = 3;
        let n_resumes = 2; // not enough

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(3)
            .failures([RETRYABLE, RETRYABLE, RETRYABLE])
            .success_with_stream_failure(2)
            .success_with_stream_failure(0) // no progress
            .failure(RETRYABLE)
            .success_with_stream_failure(1)
            .success_with_stream_failure(0) // no progress
            .success_with_stream_failure(1)
            .failures([RETRYABLE, RETRYABLE, RETRYABLE])
            .success_with_stream_failure(0) // no progress
            .success_with_stream_failure(1)
            .failures([RETRYABLE, RETRYABLE, NON_RETRYABLE])
            .never();

        let (num_retries, stream_resume_attempts, received) = download(
            client_builder,
            n_retries,
            n_resumes,
            InclusiveRange(0, BLOB.len() as u64 - 1),
        )
        .await
        .unwrap();

        assert_eq!(num_retries, 9, "num_retries");
        assert_eq!(stream_resume_attempts, 8, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..8].to_vec()));
    }

    const BLOB: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    fn get_builder() -> FailingClientSimulatorBuilder {
        FailingClientSimulatorBuilder::default()
            .blob_static(BLOB)
            .chunk_size(3)
    }

    /// returns (num_retries, stream_resume_attempts, collected_bytes)
    async fn download<S: Into<InclusiveRange>, B: Into<FailingClientSimulatorBuilder>>(
        client_builder: B,
        n_retries: usize,
        n_resumes: usize,
        download_spec: S,
    ) -> Result<(usize, usize, Result<Vec<u8>, Vec<u8>>), CondowError> {
        let config = RetryConfig::default()
            .max_attempts(n_retries)
            .max_stream_resume_attempts(n_resumes)
            .max_delay_ms(0);

        download_with_config(client_builder, config, download_spec).await
    }

    /// returns (num_retries, stream_resume_attempts, collected_bytes)
    async fn download_with_config<
        S: Into<InclusiveRange>,
        B: Into<FailingClientSimulatorBuilder>,
    >(
        client_builder: B,
        config: RetryConfig,
        range: S,
    ) -> Result<(usize, usize, Result<Vec<u8>, Vec<u8>>), CondowError> {
        let client = client_builder.into().finish();

        #[derive(Clone, Default)]
        struct TestProbe(Arc<AtomicUsize>, Arc<AtomicUsize>);

        impl Probe for TestProbe {
            fn retry_attempt(&self, _error: &CondowError, _next_in: Duration) {
                // Count the number of retries
                self.0.as_ref().fetch_add(1, Ordering::SeqCst);
            }

            fn stream_resume_attempt(
                &self,
                _error: &CondowError,
                _orig_range: InclusiveRange,
                _remaining_range: InclusiveRange,
            ) {
                // Count the number of broken streams
                self.1.as_ref().fetch_add(1, Ordering::SeqCst);
            }
        }

        let probe = TestProbe::default();

        let mut stream = RetryPartStream::from_client(
            &client,
            IgnoreLocation,
            range.into(),
            config,
            probe.clone(),
        )
        .await?;

        let mut received = Vec::new();

        while let Some(next) = stream.next().await {
            match next {
                Ok(bytes) => received.extend_from_slice(&bytes),
                Err(_err) => {
                    return Ok((
                        probe.0.load(Ordering::SeqCst),
                        probe.1.load(Ordering::SeqCst),
                        Err(received),
                    ))
                }
            }
        }

        Ok((
            probe.0.load(Ordering::SeqCst),
            probe.1.load(Ordering::SeqCst),
            Ok(received),
        ))
    }
}
mod retry_resume_part_stream {
    //! Tests the [RetryResumePartStream]
    //!
    //! No retries are tested/used for getting a [BytesStream]

    use std::{
        ops::RangeInclusive,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use futures::{FutureExt, StreamExt};

    use crate::retry::retry_stream::RetryResumePartStream;
    use crate::{
        condow_client::{
            failing_client_simulator::FailingClientSimulatorBuilder, CondowClient, IgnoreLocation,
        },
        config::RetryConfig,
        errors::CondowError,
        probe::Probe,
        retry::tests::RETRYABLE,
        InclusiveRange,
    };

    #[tokio::test]
    async fn full_range_no_error() {
        let n_retries = 0;
        let client_builder = get_builder();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn full_range_1_broken_stream_with_resume() {
        let n_retries = 0;
        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 1, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn full_range_1_broken_stream_without_resume() {
        let config = RetryConfig::default()
            .max_attempts(0)
            .no_stream_resume_attempts()
            .max_delay_ms(0);
        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download_with_config(client_builder, config, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 0, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..5].to_vec()));
    }

    #[tokio::test]
    async fn full_range_2_broken_streams_with_resume() {
        let n_retries = 1;
        let client_builder = get_builder()
            .responses()
            .successes_with_stream_failure([5, 9])
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 2, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn full_range_3_broken_streams_with_resume() {
        let n_retries = 1;
        let client_builder = get_builder()
            .responses()
            .successes_with_stream_failure([5, 9, 15])
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 3, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn full_range_4_broken_streams_with_resume() {
        let n_retries = 1;
        let client_builder = get_builder()
            .responses()
            .successes_with_stream_failure([0, 5, 9, 15])
            .success()
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 4, "stream_resume_attempts");
        assert_eq!(received, Ok(BLOB.to_vec()));
    }

    #[tokio::test]
    async fn consumption_aborts_after_n_resume_attempts() {
        for max_resumes in 0..4 {
            let config = RetryConfig::default()
                .max_attempts(0)
                .max_stream_resume_attempts(max_resumes)
                .max_delay_ms(0);
            let client_builder = get_builder()
                .responses()
                .successes_with_stream_failure([5, 0, 0, 0, 0])
                .never();

            let (num_retries, stream_resume_attempts, received) =
                download_with_config(client_builder, config, FULL_RANGE).await;

            assert_eq!(num_retries, 0, "num_retries");
            assert_eq!(
                stream_resume_attempts, max_resumes,
                "max stream_resume_attempts ({})",
                max_resumes
            );
            assert_eq!(received, Err(BLOB[0..5].to_vec()), "result {}", max_resumes);
        }
    }

    #[tokio::test]
    async fn full_range_1_retryable_error_with_0_retries_with_resume() {
        let n_retries = 0;
        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(5)
            .failure(RETRYABLE)
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download(client_builder, n_retries, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 1, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..5].to_vec()));
    }

    #[tokio::test]
    async fn full_range_fail_on_resume_no_progress() {
        let config = RetryConfig::default()
            .max_stream_resume_attempts(2)
            .max_delay_ms(0);

        let client_builder = get_builder()
            .responses()
            .success_with_stream_failure(3)
            // resume 1
            .success_with_stream_failure(2) // makes 2 bytes progress
            // resume 2
            .success_with_stream_failure(0) // makes 0 bytes progress (first failed resume)
            // resume 3
            .success_with_stream_failure(3) // makes 3 bytes progress
            // resume 4
            .success_with_stream_failure(0) // makes 0 bytes progress (first failed resume)
            // resume 5
            .success_with_stream_failure(0) // makes 0 bytes progress (second failed resume, abort!)
            .never();

        let (num_retries, stream_resume_attempts, received) =
            download_with_config(client_builder, config, FULL_RANGE).await;

        assert_eq!(num_retries, 0, "num_retries");
        assert_eq!(stream_resume_attempts, 5, "stream_resume_attempts");
        assert_eq!(received, Err(BLOB[0..8].to_vec()));
    }

    const BLOB: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    const FULL_RANGE: RangeInclusive<u64> = 0u64..=(BLOB.len() - 1) as u64;

    fn get_builder() -> FailingClientSimulatorBuilder {
        FailingClientSimulatorBuilder::default()
            .blob_static(BLOB)
            .chunk_size(3)
    }

    /// returns (num_retries, stream_resume_attempts, collected_bytes)
    async fn download<R: Into<InclusiveRange>, B: Into<FailingClientSimulatorBuilder>>(
        client_builder: B,
        n_retries: usize,
        range: R,
    ) -> (usize, usize, Result<Vec<u8>, Vec<u8>>) {
        let config = RetryConfig::default()
            .max_attempts(n_retries)
            .max_delay_ms(0);

        download_with_config(client_builder, config, range).await
    }

    /// returns (num_retries, stream_resume_attempts, collected_bytes)
    async fn download_with_config<
        R: Into<InclusiveRange>,
        B: Into<FailingClientSimulatorBuilder>,
    >(
        client_builder: B,
        config: RetryConfig,
        range: R,
    ) -> (usize, usize, Result<Vec<u8>, Vec<u8>>) {
        let client = client_builder.into().finish();

        #[derive(Clone, Default)]
        struct TestProbe(Arc<AtomicUsize>, Arc<AtomicUsize>);

        impl Probe for TestProbe {
            fn retry_attempt(&self, _error: &CondowError, _next_in: Duration) {
                // Count the number of retries
                self.0.as_ref().fetch_add(1, Ordering::SeqCst);
            }

            fn stream_resume_attempt(
                &self,
                _error: &CondowError,
                _orig_range: InclusiveRange,
                _remaining_range: InclusiveRange,
            ) {
                // Count the number of broken streams
                self.1.as_ref().fetch_add(1, Ordering::SeqCst);
            }
        }

        let probe = TestProbe::default();

        let original_range: InclusiveRange = range.into();
        let initial_stream = client
            .download(IgnoreLocation, original_range)
            .await
            .unwrap();

        let get_stream_fn = {
            let client = client.clone();
            move |range: InclusiveRange| client.download(IgnoreLocation, range).boxed()
        };

        let mut retry_stream = RetryResumePartStream::new(
            original_range,
            initial_stream,
            Arc::new(get_stream_fn),
            config,
            Arc::new(probe.clone()),
        );

        let mut received = Vec::new();

        while let Some(next) = retry_stream.next().await {
            if let Ok(bytes) = next {
                received.extend_from_slice(&bytes);
            } else {
                return (
                    probe.0.load(Ordering::SeqCst),
                    probe.1.load(Ordering::SeqCst),
                    Err(received),
                );
            }
        }

        (
            probe.0.load(Ordering::SeqCst),
            probe.1.load(Ordering::SeqCst),
            Ok(received),
        )
    }
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

    use futures::FutureExt;

    use crate::{
        condow_client::IgnoreLocation, config::retry_stream::gen_retry_get_stream_fn,
        errors::CondowErrorKind,
    };

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
            let expected = Err((0, kind)); // no retries => always fails

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
    async fn only_one_call_made_when_no_retries() {
        let n_retries = 0;

        let errors = vec![RETRYABLE, ANOTHER_RETRYABLE];
        let result = run_get_stream(errors, n_retries).await;
        assert_eq!(result, Err((0, RETRYABLE)));
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
            type Location = IgnoreLocation;

            fn get_size(
                &self,
                _location: Self::Location,
            ) -> futures::future::BoxFuture<'static, Result<u64, CondowError>> {
                unimplemented!()
            }

            fn download(
                &self,
                _location: Self::Location,
                _range: InclusiveRange,
            ) -> futures::future::BoxFuture<'static, Result<ClientBytesStream, CondowError>>
            {
                let mut fails = self.fails_reversed.lock().unwrap();

                if fails.is_empty() {
                    futures::future::ready(Ok(ClientBytesStream::empty())).boxed()
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
        struct TestProbe(Arc<AtomicUsize>);

        impl Probe for TestProbe {
            fn retry_attempt(&self, _error: &CondowError, _next_in: Duration) {
                // Count the number of retries
                self.0.as_ref().fetch_add(1, Ordering::SeqCst);
            }
        }

        let config = RetryConfig::default()
            .max_attempts(n_retries)
            .max_delay_ms(0);

        let probe = TestProbe(Default::default());

        let get_part_stream = {
            let client = client.clone();
            move |range: InclusiveRange| client.download(IgnoreLocation, range).boxed()
        };

        match gen_retry_get_stream_fn(Arc::new(get_part_stream), config, Arc::new(probe.clone()))(
            InclusiveRange(0, 42),
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

    use crate::{condow_client::IgnoreLocation, errors::CondowErrorKind};

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
            let expected = Err((0, kind)); // no retries => always fails

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
    async fn only_one_call_made_when_no_retries() {
        let n_retries = 0;

        let errors = vec![RETRYABLE, ANOTHER_RETRYABLE];
        let result = run_get_size(errors, n_retries).await;
        assert_eq!(result, Err((0, RETRYABLE)));
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
            type Location = IgnoreLocation;

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
                _range: InclusiveRange,
            ) -> futures::future::BoxFuture<'static, Result<ClientBytesStream, CondowError>>
            {
                unimplemented!()
            }
        }

        fails.reverse();
        let client = Client {
            fails_reversed: Arc::new(Mutex::new(fails)),
        };

        #[derive(Clone)]
        struct TestProbe(Arc<AtomicUsize>);

        impl Probe for TestProbe {
            fn retry_attempt(&self, _error: &CondowError, _next_in: Duration) {
                // Count the number of retries
                self.0.as_ref().fetch_add(1, Ordering::SeqCst);
            }
        }

        let config = RetryConfig::default()
            .max_attempts(n_retries)
            .max_delay_ms(0);

        let probe = TestProbe(Default::default());
        match retry_get_size(&client, IgnoreLocation, &config, &probe.clone()).await {
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
