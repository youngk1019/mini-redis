use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

pub(crate) struct Crc64AsyncWriter<W: AsyncWrite> {
    inner: W,
    crc64_hasher: crc64fast::Digest,
}

impl<W: AsyncWrite> Crc64AsyncWriter<W> {
    pub fn new(inner: W) -> Crc64AsyncWriter<W> {
        Crc64AsyncWriter {
            inner,
            crc64_hasher: crc64fast::Digest::new(),
        }
    }

    pub fn crc64(&self) -> u64 {
        self.crc64_hasher.sum64()
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Crc64AsyncWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let self_mut = self.get_mut();
        self_mut.crc64_hasher.write(buf);
        Pin::new(&mut self_mut.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}