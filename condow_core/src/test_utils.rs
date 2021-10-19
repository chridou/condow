use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use futures::{
    future::{self, BoxFuture, FutureExt, TryFutureExt},
    stream, StreamExt as _,
};
use rand::{prelude::SliceRandom, rngs::OsRng, Rng};
use tokio::time;

use crate::{
    condow_client::{CondowClient, DownloadSpec},
    errors::CondowError,
    reader::RandomAccessReader,
    streams::{BytesHint, BytesStream, Chunk, ChunkStream, ChunkStreamItem, PartStream},
    DownloadRange, Downloads,
};

#[derive(Debug, Clone)]
pub struct NoLocation;

impl std::fmt::Display for NoLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoLocation")
    }
}

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
                self.data.len(),
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
pub struct TestDownloader {
    blob: Arc<Mutex<Vec<u8>>>,
    pattern: Arc<Mutex<Vec<Option<usize>>>>,
}

impl TestDownloader {
    /// Create a new [TestDownloader] with a BLOB of size `len`.
    ///
    /// Cycles the bytes starting with 0.
    pub fn new(len: usize) -> Self {
        let mut blob = Vec::with_capacity(len);
        for n in 0..len {
            blob.push(n as u8)
        }
        Self {
            blob: Arc::new(Mutex::new(blob)),
            pattern: Arc::new(Mutex::new(vec![Some(2), Some(5), Some(3), Some(7)])),
        }
    }

    /// Create a new [TestDownloader] using the given BLOB.
    pub fn new_with_blob(blob: Vec<u8>) -> Self {
        Self {
            blob: Arc::new(Mutex::new(blob)),
            pattern: Arc::new(Mutex::new(vec![Some(5), Some(3), Some(7)])),
        }
    }

    pub fn set_pattern(self, pattern: Vec<Option<usize>>) {
        *self.pattern.lock().unwrap() = pattern;
    }

    pub fn blob(&self) -> Vec<u8> {
        self.blob.lock().unwrap().clone()
    }
}

impl Downloads<usize> for TestDownloader {
    fn download<'a, R: Into<DownloadRange> + Send + Sync + 'static>(
        &'a self,
        location: usize,
        range: R,
    ) -> BoxFuture<'a, Result<crate::streams::PartStream<crate::streams::ChunkStream>, CondowError>>
    {
        self.download_chunks(location, range)
            .and_then(|st| async move { PartStream::from_chunk_stream(st) })
            .boxed()
    }

    fn download_chunks<'a, R: Into<crate::DownloadRange> + Send + Sync + 'static>(
        &'a self,
        _location: usize,
        range: R,
    ) -> BoxFuture<'a, Result<crate::streams::ChunkStream, CondowError>> {
        Box::pin(make_a_stream(
            self.blob.as_ref(),
            range.into(),
            self.pattern.lock().unwrap().clone(),
        ))
    }

    fn get_size<'a>(&'a self, _location: usize) -> BoxFuture<'a, Result<u64, CondowError>> {
        let len = self.blob.lock().unwrap().len();
        futures::future::ok(len as u64).boxed()
    }

    fn reader_with_length(&self, location: usize, length: u64) -> RandomAccessReader<Self, usize>
    where
        Self: Sized,
    {
        RandomAccessReader::new_with_length(self.clone(), location, length)
    }
}

async fn make_a_stream(
    blob: &Mutex<Vec<u8>>,
    range: DownloadRange,
    pattern: Vec<Option<usize>>,
) -> Result<ChunkStream, CondowError> {
    let blob_guard = blob.lock().unwrap();
    let range_incl = if let Some(range) = range.incl_range_from_size(blob_guard.len() as u64) {
        range
    } else {
        return Err(CondowError::new_invalid_range("invalid range"));
    };

    let range = range_incl.start() as usize..(range_incl.end_incl() + 1) as usize;

    let bytes = blob_guard[range].to_vec();

    drop(blob_guard);

    let mut chunk_patterns = pattern.into_iter().cycle().enumerate();

    let (chunk_stream, tx) = ChunkStream::new(BytesHint::new_exact(bytes.len() as u64));

    tokio::spawn(async move {
        let mut start = 0;
        loop {
            if start == bytes.len() {
                return;
            }

            let (chunk_index, chunk_len) =
                if let Some((chunk_index, pattern)) = chunk_patterns.next() {
                    if let Some(pattern) = pattern {
                        (chunk_index, pattern)
                    } else {
                        let _ = tx.unbounded_send(Err(CondowError::new_other("test error")));
                        break;
                    }
                } else {
                    break;
                };

            let end_excl = bytes.len().min(start + chunk_len);
            let chunk = Bytes::copy_from_slice(&bytes[start..end_excl]);

            let bytes_left = (bytes.len() - end_excl) as u64;
            let chunk = Chunk {
                part_index: 0,
                chunk_index,
                blob_offset: start as u64,
                range_offset: start as u64,
                bytes: chunk,
                bytes_left,
            };

            let _ = tx.unbounded_send(Ok(chunk));

            start = end_excl;
        }
    });

    Ok(chunk_stream)
}

#[tokio::test]
async fn check_test_downloader() {
    for n in 1..255 {
        let expected: Vec<u8> = (0..n).collect();

        let downloader = TestDownloader::new(n as usize);

        let parts = downloader.download(42, ..).await.unwrap();

        let result = parts.into_vec().await.unwrap();

        assert_eq!(result, expected, "bytes read ({} items)", n);
    }
}
