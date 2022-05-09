use std::time::Instant;

use futures::{StreamExt, TryStreamExt};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    condow_client::CondowClient, config::ClientRetryWrapper, errors::CondowError,
    machinery::part_request::PartRequestIterator, probe::Probe, streams::Chunk,
};

pub(crate) async fn download_chunks_sequentially<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
    sender: UnboundedSender<Result<Chunk, CondowError>>,
) {
    probe.download_started();
    let download_started_at = Instant::now();
    for part_request in part_requests {
        let part_started_at = Instant::now();
        probe.part_started(part_request.part_index, part_request.blob_range);
        let (stream, _bytes_hint) = client
            .download(location.clone(), part_request.blob_range.into(), &probe)
            .await
            .unwrap();
        let mut chunk_index = 0;
        let mut range_offset = part_request.range_offset;
        let mut blob_offset = part_request.blob_range.start();
        let mut bytes_left = part_request.blob_range.len();
        let mut stream = stream
            .map_ok(|bytes| {
                let bytes_len = bytes.len() as u64;
                bytes_left -= bytes_len;
                let chunk = Chunk {
                    part_index: part_request.part_index,
                    chunk_index,
                    blob_offset,
                    range_offset,
                    bytes,
                    bytes_left,
                };
                chunk_index += 1;
                blob_offset += bytes_len;
                range_offset += bytes_len;
                chunk
            })
            .map_err(Into::into);
        let mut n_chunks = 0;
        let mut chunk_started_at = Instant::now();
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    probe.chunk_completed(
                        part_request.part_index,
                        chunk.chunk_index,
                        chunk.bytes.len(),
                        chunk_started_at.elapsed(),
                    );
                    chunk_started_at = Instant::now();
                    n_chunks += 1;
                    let send_result = sender
                        .send(Ok(chunk))
                        .map_err(|e| CondowError::new_io(e.to_string()));
                    if let Err(err) = send_result {
                        probe.part_failed(&err, part_request.part_index, &part_request.blob_range);
                        probe.download_failed(Some(download_started_at.elapsed()));
                        return;
                    }
                }
                Err(chunk_error) => {
                    probe.part_failed(
                        &chunk_error,
                        part_request.part_index,
                        &part_request.blob_range,
                    );
                    probe.download_failed(Some(download_started_at.elapsed()));
                    sender.send(Err(chunk_error)).ok();
                    return;
                }
            }
        }
        probe.part_completed(
            part_request.part_index,
            n_chunks,
            part_request.blob_range.len(),
            part_started_at.elapsed(),
        );
    }
    probe.download_completed(download_started_at.elapsed());
}
