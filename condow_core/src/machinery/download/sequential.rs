use std::{task::Poll, time::Instant};

use futures::{stream::BoxStream, Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::{
    condow_client::CondowClient,
    config::ClientRetryWrapper,
    errors::CondowError,
    machinery::part_request::{PartRequest, PartRequestIterator},
    probe::Probe,
    streams::{BytesHint, BytesStream, Chunk, ChunkStream, ChunkStreamItem},
    InclusiveRange,
};

pub(crate) async fn download_chunks_sequentially<C: CondowClient, P: Probe + Clone>(
    part_requests: PartRequestIterator,
    client: ClientRetryWrapper<C>,
    location: C::Location,
    probe: P,
) -> ChunkStream {
    probe.download_started();
    let bytes_hint = part_requests.bytes_hint();
    let poll_parts = PollPartsSeq::new(client, location, part_requests, probe);

    let chunk_stream = ChunkStream::from_stream(poll_parts.boxed(), bytes_hint);

    chunk_stream
}

struct StreamingState {
    bytes_stream: BytesStream,
    part_index: u64,
    chunk_index: usize,
    blob_offset: u64,
    range_offset: u64,
    bytes_left: u64,
    bytes_streams: BoxStream<'static, Result<(BytesStream, BytesHint, PartRequest), CondowError>>,
    part_started_at: Instant,
    part_range: InclusiveRange,
}

enum State {
    WaitingForStream {
        bytes_streams:
            BoxStream<'static, Result<(BytesStream, BytesHint, PartRequest), CondowError>>,
    },
    Streaming(StreamingState),
    Finished,
}

pin_project! {
struct PollPartsSeq<P> {
    state: State,
    probe: P,
    download_started_at: Instant,
}
}

impl<P> PollPartsSeq<P>
where
    P: Probe + Clone,
{
    fn new<I, C>(
        client: ClientRetryWrapper<C>,
        location: C::Location,
        part_requests: I,
        probe: P,
    ) -> Self
    where
        I: Iterator<Item = PartRequest> + Send + 'static,
        C: CondowClient,
    {
        let bytes_streams = yield_bytes_streams::YieldBytesStreams::new(
            client,
            location,
            part_requests,
            probe.clone(),
        );

        Self {
            state: State::WaitingForStream {
                bytes_streams: bytes_streams.boxed(),
            },
            probe,
            download_started_at: Instant::now(),
        }
    }
}

impl<P> Stream for PollPartsSeq<P>
where
    P: Probe + Clone,
{
    type Item = ChunkStreamItem;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let state = std::mem::replace(this.state, State::Finished);
        match state {
            State::Finished => Poll::Ready(None),
            State::WaitingForStream { mut bytes_streams } => {
                match bytes_streams.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok((bytes_stream, _, part_request)))) => {
                        *this.state = State::Streaming(StreamingState {
                            bytes_stream,
                            part_index: part_request.part_index,
                            chunk_index: 0,
                            blob_offset: part_request.blob_range.start(),
                            range_offset: part_request.range_offset,
                            bytes_left: part_request.blob_range.len(),
                            bytes_streams,
                            part_started_at: Instant::now(),
                            part_range: part_request.blob_range,
                        });
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(Some(Err(err))) => {
                        this.probe
                            .download_failed(Some(this.download_started_at.elapsed()));
                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                    Poll::Ready(None) => {
                        *this.state = State::Finished;
                        Poll::Ready(None)
                    }
                    Poll::Pending => {
                        *this.state = State::WaitingForStream { bytes_streams };
                        Poll::Pending
                    }
                }
            }
            State::Streaming(mut streaming_state) => {
                match streaming_state.bytes_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(bytes))) => {
                        let bytes_len = bytes.len() as u64;
                        streaming_state.bytes_left -= bytes_len;
                        let chunk = Chunk {
                            part_index: streaming_state.part_index,
                            chunk_index: streaming_state.chunk_index,
                            blob_offset: streaming_state.blob_offset,
                            range_offset: streaming_state.range_offset,
                            bytes,
                            bytes_left: streaming_state.bytes_left,
                        };

                        this.probe.chunk_completed(
                            streaming_state.part_index,
                            streaming_state.chunk_index,
                            bytes_len as usize,
                        );

                        streaming_state.chunk_index += 1;
                        streaming_state.blob_offset += bytes_len;
                        streaming_state.range_offset += bytes_len;
                        *this.state = State::Streaming(streaming_state);
                        Poll::Ready(Some(Ok(chunk)))
                    }
                    Poll::Ready(Some(Err(err))) => {
                        let err: CondowError = err.into();
                        this.probe.part_failed(
                            &err,
                            streaming_state.part_index,
                            &streaming_state.part_range,
                        );
                        this.probe
                            .download_failed(Some(this.download_started_at.elapsed()));
                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                    Poll::Ready(None) => {
                        this.probe.part_completed(
                            streaming_state.part_index,
                            streaming_state.chunk_index,
                            streaming_state.part_range.len(),
                            streaming_state.part_started_at.elapsed(),
                        );
                        *this.state = State::WaitingForStream {
                            bytes_streams: streaming_state.bytes_streams,
                        };
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Pending => {
                        *this.state = State::Streaming(streaming_state);
                        Poll::Pending
                    }
                }
            }
        }
    }
}

mod yield_bytes_streams {
    use std::task::Poll;

    use futures::{future::BoxFuture, FutureExt, Stream};
    use pin_project_lite::pin_project;

    use crate::{
        condow_client::CondowClient,
        errors::CondowError,
        machinery::part_request::PartRequest,
        probe::Probe,
        retry::ClientRetryWrapper,
        streams::{BytesHint, BytesStream},
    };

    enum State {
        Start,
        Finished,
        GettingStream {
            fut: BoxFuture<'static, Result<(BytesStream, BytesHint), CondowError>>,
            part_request: PartRequest,
        },
    }

    pin_project! {
        pub struct YieldBytesStreams<C: CondowClient, P> {
            client: ClientRetryWrapper<C>,
            location: C::Location,
            part_requests: Box<dyn Iterator<Item=PartRequest>+ Send + 'static>,
            state: State,
            probe: P,
        }
    }

    impl<C, P> YieldBytesStreams<C, P>
    where
        C: CondowClient,
        P: Probe + Clone,
    {
        pub(crate) fn new<I>(
            client: ClientRetryWrapper<C>,
            location: C::Location,
            part_requests: I,
            probe: P,
        ) -> Self
        where
            I: Iterator<Item = PartRequest> + Send + 'static,
        {
            YieldBytesStreams {
                client,
                location,
                part_requests: Box::new(part_requests),
                state: State::Start,
                probe,
            }
        }
    }

    impl<C, P> Stream for YieldBytesStreams<C, P>
    where
        C: CondowClient,
        P: Probe + Clone,
    {
        type Item = Result<(BytesStream, BytesHint, PartRequest), CondowError>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.project();
            let state = std::mem::replace(this.state, State::Finished);
            match state {
                State::Start => {
                    if let Some(part_request) = this.part_requests.next() {
                        let fut = this
                            .client
                            .download(
                                this.location.clone(),
                                part_request.blob_range.into(),
                                this.probe.clone(),
                            )
                            .boxed();
                        *this.state = State::GettingStream { fut, part_request };
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    } else {
                        *this.state = State::Finished;
                        Poll::Ready(None)
                    }
                }
                State::GettingStream {
                    mut fut,
                    part_request,
                } => match fut.poll_unpin(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok((stream, bytes_hint))) => {
                        *this.state = State::Start;
                        Poll::Ready(Some(Ok((stream, bytes_hint, part_request))))
                    }
                    Poll::Ready(Err(err)) => {
                        *this.state = State::Finished;
                        Poll::Ready(Some(Err(err)))
                    }
                },
                State::Finished => Poll::Ready(None),
            }
        }
    }
}
