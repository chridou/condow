use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::{
    channel::mpsc,
    future::{self, BoxFuture, FutureExt},
    stream, StreamExt as _,
};
use rand::{prelude::SliceRandom, rngs::OsRng, Rng};
use tokio::time;

use crate::{
    condow_client::{CondowClient, DownloadSpec, NoLocation},
    config::Config,
    errors::{CondowError, IoError},
    streams::{BytesHint, BytesStream, Chunk, ChunkStream, ChunkStreamItem, PartStream},
    Condow,
};

#[derive(Clone)]
pub struct TestCondowClient {
    pub data: Arc<Vec<u8>>,
    pub max_jitter_ms: usize,
    pub include_size_hint: bool,
    pub max_chunk_size: usize,
}

impl TestCondowClient {
    pub fn new() -> Self {
        Self {
            data: Arc::new(create_test_data()),
            max_jitter_ms: 0,
            include_size_hint: true,
            max_chunk_size: 10,
        }
    }

    pub fn max_jitter_ms(mut self, max_jitter_ms: usize) -> Self {
        self.max_jitter_ms = max_jitter_ms;
        self
    }

    pub fn max_chunk_size(mut self, max_chunk_size: usize) -> Self {
        self.max_chunk_size = max_chunk_size;
        self
    }

    pub fn include_size_hint(mut self, include_size_hint: bool) -> Self {
        self.include_size_hint = include_size_hint;
        self
    }

    pub fn data(&self) -> Arc<Vec<u8>> {
        Arc::clone(&self.data)
    }
}

impl Default for TestCondowClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CondowClient for TestCondowClient {
    type Location = NoLocation;

    fn get_size(
        &self,
        _location: Self::Location,
    ) -> BoxFuture<'static, Result<u64, crate::errors::CondowError>> {
        let f = future::ready(Ok(self.data.len() as u64));
        Box::pin(f)
    }

    fn download(
        &self,
        _location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<
        'static,
        Result<
            (crate::streams::BytesStream, crate::streams::BytesHint),
            crate::errors::CondowError,
        >,
    > {
        let range = match spec {
            DownloadSpec::Complete => 0..self.data.len(),
            DownloadSpec::Range(r) => {
                let r = r.to_std_range_excl();
                r.start as usize..r.end as usize
            }
        };

        if range.end > self.data.len() {
            return Box::pin(future::ready(Err(CondowError::new_invalid_range(format!(
                "max upper bound is {} but {} was requested",
                self.data.len() - 1,
                range.end - 1
            )))));
        }

        let slice = &self.data[range];

        let bytes_hint = if self.include_size_hint {
            BytesHint::new_exact(slice.len() as u64)
        } else {
            BytesHint::new_no_hint()
        };

        let iter = slice
            .chunks(self.max_chunk_size)
            .map(Bytes::copy_from_slice)
            .map(Ok);

        let owned_bytes: Vec<_> = iter.collect();

        let jitter = self.max_jitter_ms;
        let stream = stream::iter(owned_bytes).then(move |bytes| async move {
            if jitter > 0 {
                let jitter = OsRng.gen_range(0..=(jitter as u64));
                time::sleep(Duration::from_millis(jitter)).await;
            }
            bytes
        });

        let stream: BytesStream = Box::pin(stream);

        let f = future::ready(Ok((stream, bytes_hint)));

        Box::pin(f)
    }
}

pub fn create_test_data() -> Vec<u8> {
    let mut data: Vec<u8> = Vec::new();

    for n in 1u8..=255 {
        let bytes = n.to_be_bytes();
        data.extend_from_slice(bytes.as_ref());
    }
    data
}

pub fn create_chunk_stream(
    n_parts: u64,
    n_chunks: usize,
    exact_hint: bool,
    max_variable_chunk_size: Option<usize>,
) -> (ChunkStream, Vec<u8>) {
    let mut values = Vec::new();
    let value = &mut 1u8;
    let mut get_next = || {
        let v = *value;
        if *value == 255 {
            *value = 1;
        } else {
            *value += 1;
        }
        values.push(v);
        v
    };

    let mut rng = rand::thread_rng();
    let blob_offset: u64 = rng.gen_range(0..1_000);

    let mut parts: Vec<Vec<ChunkStreamItem>> = Vec::new();

    let mut range_offset: u64 = 0;
    for part_index in 0u64..n_parts {
        let mut chunks = Vec::with_capacity(n_chunks);

        let mut chunks_bytes = Vec::with_capacity(n_chunks);

        for chunk_index in 0usize..n_chunks {
            let bytes = if let Some(max) = max_variable_chunk_size {
                let n = rng.gen_range(1..=max);
                let mut items = Vec::with_capacity(n);
                (0..n).for_each(|_| items.push(get_next()));
                Bytes::from(items)
            } else {
                Bytes::from(vec![get_next()])
            };
            chunks_bytes.push((chunk_index, bytes));
        }

        let mut bytes_left_in_part: u64 = chunks_bytes
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum();

        for (chunk_index, bytes) in chunks_bytes {
            let n_bytes = bytes.len() as u64;
            bytes_left_in_part -= n_bytes;
            let chunk = Chunk {
                part_index,
                chunk_index,
                blob_offset: blob_offset + range_offset,
                range_offset,
                bytes,
                bytes_left: bytes_left_in_part,
            };
            chunks.push(Ok(chunk));
            range_offset += n_bytes;
        }
        parts.push(chunks);
    }

    let bytes_hint = if exact_hint {
        BytesHint::new_exact(values.len() as u64)
    } else {
        BytesHint::new_at_max(values.len() as u64)
    };

    parts.shuffle(&mut rng);

    let (chunk_stream, tx) = ChunkStream::new(bytes_hint);

    loop {
        let n = rng.gen_range(0..parts.len());

        let part = &mut parts[n];
        let chunk = part.remove(0);
        let _ = tx.unbounded_send(chunk);
        if part.is_empty() {
            parts.remove(n);
        }

        if parts.is_empty() {
            break;
        }
    }

    (chunk_stream, values)
}

pub fn create_chunk_stream_with_err(
    n_parts: u64,
    n_chunks: usize,
    exact_hint: bool,
    max_variable_chunk_size: Option<usize>,
    err_at_chunk: usize,
) -> (ChunkStream, Vec<u8>) {
    let mut chunks_left_until_err = err_at_chunk;

    let mut values = Vec::new();
    let value = &mut 1u8;
    let mut get_next = || {
        let v = *value;
        if *value == 255 {
            *value = 1;
        } else {
            *value += 1;
        }
        values.push(v);
        v
    };

    let mut rng = rand::thread_rng();
    let blob_offset: u64 = rng.gen_range(0..1_000);

    let mut parts: Vec<Vec<ChunkStreamItem>> = Vec::new();

    let mut range_offset: u64 = 0;
    for part_index in 0u64..n_parts {
        let mut chunks = Vec::with_capacity(n_chunks);

        let mut chunks_bytes = Vec::with_capacity(n_chunks);

        for chunk_index in 0usize..n_chunks {
            let bytes = if let Some(max) = max_variable_chunk_size {
                let n = rng.gen_range(1..=max);
                let mut items = Vec::with_capacity(n);
                (0..n).for_each(|_| items.push(get_next()));
                Bytes::from(items)
            } else {
                Bytes::from(vec![get_next()])
            };
            chunks_bytes.push((chunk_index, bytes));
        }

        let mut bytes_left_in_part: u64 = chunks_bytes
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum();

        for (chunk_index, bytes) in chunks_bytes {
            let n_bytes = bytes.len() as u64;
            bytes_left_in_part -= n_bytes;
            let chunk = Chunk {
                part_index,
                chunk_index,
                blob_offset: blob_offset + range_offset,
                range_offset,
                bytes,
                bytes_left: bytes_left_in_part,
            };
            chunks.push(Ok(chunk));
            range_offset += n_bytes;
        }
        parts.push(chunks);
    }

    let bytes_hint = if exact_hint {
        BytesHint::new_exact(values.len() as u64)
    } else {
        BytesHint::new_at_max(values.len() as u64)
    };

    parts.shuffle(&mut rng);

    let (chunk_stream, tx) = ChunkStream::new(bytes_hint);

    let mut err_sent = chunks_left_until_err == 0;
    loop {
        if chunks_left_until_err == 0 {
            let _ = tx.unbounded_send(Err(CondowError::new_other("forced stream error")));
            err_sent = true;
            break;
        }
        chunks_left_until_err -= 1;
        let n = rng.gen_range(0..parts.len());

        let part = &mut parts[n];
        let chunk = part.remove(0);
        let _ = tx.unbounded_send(chunk);
        if part.is_empty() {
            parts.remove(n);
        }

        if parts.is_empty() {
            break;
        }
    }

    if !err_sent {
        let _ = tx.unbounded_send(Err(CondowError::new_other("forced stream error after end")));
    }

    (chunk_stream, values)
}

pub fn create_part_stream(
    n_parts: u64,
    n_chunks: usize,
    exact_hint: bool,
    max_variable_chunk_size: Option<usize>,
) -> (PartStream<ChunkStream>, Vec<u8>) {
    let (stream, values) =
        create_chunk_stream(n_parts, n_chunks, exact_hint, max_variable_chunk_size);
    (PartStream::from_chunk_stream(stream).unwrap(), values)
}

#[tokio::test]
async fn check_chunk_stream_fixed_chunk_size() {
    let (stream, expected) = create_chunk_stream(10, 10, true, None);

    let result = stream.into_vec().await.unwrap();

    assert_eq!(result, expected);
}

#[tokio::test]
async fn check_chunk_stream_variable_chunk_size() {
    let (stream, expected) = create_chunk_stream(10, 10, true, Some(10));

    let result = stream.into_vec().await.unwrap();

    assert_eq!(result, expected);
}

#[derive(Clone)]
pub struct TestClient {
    blob: Arc<Vec<u8>>,
    pattern: Arc<Vec<Option<usize>>>,
}

impl TestClient {
    /// Create a new [TestDownloader] with a BLOB of size `len`.
    ///
    /// Cycles the bytes starting with 0.
    pub fn new(len: usize) -> Self {
        let mut blob = Vec::with_capacity(len);
        for n in 0..len {
            blob.push(n as u8)
        }
        Self {
            blob: Arc::new(blob),
            pattern: Arc::new(vec![Some(2), Some(5), Some(3), Some(7)]),
        }
    }

    pub fn condow(self) -> Condow<Self> {
        let config = Config::default()
            .disable_retries()
            .max_concurrency(1)
            .buffers_full_delay_ms(1)
            .part_size_bytes(2048);
        Condow::new(self, config).unwrap()
    }

    /// Create a new [TestDownloader] using the given BLOB.
    pub fn new_with_blob(blob: Vec<u8>) -> Self {
        Self {
            blob: Arc::new(blob),
            pattern: Arc::new(vec![Some(2), Some(5), Some(3), Some(7)]),
        }
    }

    pub fn set_pattern(&mut self, pattern: Vec<Option<usize>>) {
        self.pattern = Arc::new(pattern);
    }

    pub fn blob(&self) -> &[u8] {
        &self.blob
    }
}

impl CondowClient for TestClient {
    type Location = NoLocation;

    fn download(
        &self,
        _location: Self::Location,
        spec: DownloadSpec,
    ) -> BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>> {
        futures::future::ready(make_a_stream(&self.blob, spec, &self.pattern)).boxed()
    }

    fn get_size(&self, _location: NoLocation) -> BoxFuture<'static, Result<u64, CondowError>> {
        let len = self.blob.len();
        futures::future::ok(len as u64).boxed()
    }
}

fn make_a_stream(
    blob: &[u8],
    range: DownloadSpec,
    pattern: &[Option<usize>],
) -> Result<(BytesStream, BytesHint), CondowError> {
    let range = match range {
        DownloadSpec::Complete => 0..blob.len(),
        DownloadSpec::Range(r) => {
            let r = r.to_std_range_excl();
            r.start as usize..r.end as usize
        }
    };

    if range.end > blob.len() {
        return Err(CondowError::new_invalid_range(format!(
            "max upper bound is {} but {} was requested",
            blob.len() - 1,
            range.end - 1
        )));
    }

    let bytes = blob[range].to_vec();

    let mut chunk_patterns = pattern.to_vec().into_iter().cycle();

    let bytes_hint = BytesHint::new_exact(bytes.len() as u64);

    let (tx, rx) = mpsc::unbounded();

    tokio::spawn(async move {
        let mut start = 0;
        loop {
            if start == bytes.len() {
                return;
            }

            let chunk_len = if let Some(pattern) = chunk_patterns.next() {
                if let Some(pattern) = pattern {
                    pattern
                } else {
                    let _ = tx.unbounded_send(Err(IoError("test error".to_string())));
                    break;
                }
            } else {
                break;
            };

            let end_excl = bytes.len().min(start + chunk_len);
            let chunk = Bytes::copy_from_slice(&bytes[start..end_excl]);

            let _ = tx.unbounded_send(Ok(chunk));

            start = end_excl;
        }
    });

    let bytes_stream = rx.boxed();

    Ok((bytes_stream, bytes_hint))
}

#[tokio::test]
async fn check_test_downloader() {
    for n in 1..255 {
        let expected: Vec<u8> = (0..n).collect();

        let downloader = TestClient::new(n as usize).condow();

        let parts = downloader.blob().range(..).download().await.unwrap();

        let result = parts.into_vec().await.unwrap();

        assert_eq!(result, expected, "bytes read ({} items)", n);
    }
}
