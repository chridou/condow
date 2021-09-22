use std::{
    io::{Result as IoResult, SeekFrom},
    pin::Pin,
};

use futures::{AsyncRead, AsyncSeek};

use crate::{errors::CondowError, Downloads};

/// Implements [AsyncRead] and [AsyncSeek]
pub struct Reader<D, L> {
    pos: u64,
    downloader: D,
    location: L,
    length: u64,
}

impl<D, L> Reader<D, L>
where
    D: Downloads<L>,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    pub async fn new(downloader: D, location: L) -> Result<Self, CondowError> {
        let length = downloader.get_size(location.clone()).await?;
        Ok(Self {
            downloader,
            location,
            pos: 0,
            length,
        })
    }
}

impl<D, L> AsyncRead for Reader<D, L>
where
    D: Downloads<L>,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<IoResult<usize>> {
        todo!()
    }
}

impl<D, L> AsyncSeek for Reader<D, L>
where
    D: Unpin,
    L: Unpin,
{
    fn poll_seek(
        self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        pos: SeekFrom,
    ) -> std::task::Poll<IoResult<u64>> {
        let this = self.get_mut();
        match pos {
            SeekFrom::Start(pos) => this.pos = pos,
            SeekFrom::End(pos) => this.pos = (this.length as i64 + pos) as u64,
            SeekFrom::Current(pos) => this.pos = (this.pos as i64 + pos) as u64,
        };
        std::task::Poll::Ready(Ok(this.pos))
    }
}
