use std::io;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWrite, BufWriter};
use tokio::net::TcpStream;
use async_trait::async_trait;
use crate::resp::{self, Type};
use crate::cmd::Command;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

#[async_trait]
pub(crate) trait Applicable {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()>;
}

impl Connection {
    /// Create a new `Connection` instance.
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        loop {
            let maybe_frame = self.read_frame().await?;
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };
            let command: Command = frame.try_into()?;
            command.apply(self).await?;
        }
    }

    async fn read_frame(&mut self) -> crate::Result<Option<Type>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> crate::Result<Option<Type>> {
        let mut cur = Cursor::new(&self.buffer[..]);
        match Type::check(&mut cur) {
            Ok(_) => {
                let len = cur.position() as usize;
                cur.set_position(0);
                let frame = Type::parse(&mut cur)?;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(resp::Error::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_shutdown(cx)
    }
}