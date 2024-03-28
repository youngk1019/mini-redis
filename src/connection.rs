use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWrite, BufWriter};
use tokio::net::TcpStream;
use async_trait::async_trait;
use tokio::io;
use crate::resp::{self, Type};
use crate::cmd::Command;
use crate::db::DB;
use crate::utils;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    db: DB,
    writeable: bool,
    port: Option<usize>,
    id: Option<String>,
}

#[async_trait]
pub(crate) trait Applicable {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()>;
}

impl Connection {
    /// Create a new `Connection` instance.
    pub fn new(stream: TcpStream, db: DB, writeable: bool) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
            db,
            writeable,
            port: None,
            id: None,
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

    pub async fn read_frame(&mut self) -> crate::Result<Option<Type>> {
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

    pub(crate) fn db(&mut self) -> &mut DB {
        &mut self.db
    }

    pub(crate) fn writeable(&self) -> bool {
        self.writeable
    }

    pub(crate) fn set_port(&mut self, port: usize) {
        self.port = Some(port);
    }

    #[allow(dead_code)]
    pub(crate) fn set_id(&mut self, id: String) {
        self.id = Some(id);
    }

    pub(crate) async fn need_update_offset(&self) -> bool {
        self.writeable || self.db.role().await.is_master()
    }

    pub(crate) fn socket_addr(&self) -> Option<String> {
        match self.stream.get_ref().peer_addr() {
            Ok(addr) => {
                match self.port {
                    Some(port) => {
                        Some(format!("{}:{}", addr.ip(), port))
                    }
                    None => None
                }
            }
            Err(_) => None,
        }
    }

    pub(crate) fn id(&mut self) -> String {
        match &self.id {
            Some(id) => id.clone(),
            None => {
                let id = utils::strings::generate_id(Some(40));
                self.id = Some(id.clone());
                id
            }
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