use futures::{AsyncRead, AsyncSeek};

use crate::Downloads;

pub struct Reader<D, L> {
    pos: u64,
    downloader: D,
    location: L,
}

impl<D, L> Reader<D, L>
where
    D: Downloads<L>,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    pub fn new(downloader: D, location: L) -> Self {
        Self {
            downloader,
            location,
            pos: 0,
        }
    }
}

impl<D, L> AsyncRead for Reader<D, L>
where
    D: Downloads<L>,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        todo!()
    }
}

impl<D, L> AsyncSeek for Reader<D, L>
where
    D: Downloads<L>,
    L: std::fmt::Debug + std::fmt::Display + Clone + Send + Sync + 'static,
{
    fn poll_seek(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> std::task::Poll<std::io::Result<u64>> {
        todo!()
    }
}
