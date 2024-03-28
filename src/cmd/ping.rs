use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::parser::{self, Parse};
use crate::resp::Type;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;

#[derive(Debug, PartialEq)]
pub struct Ping {
    command_size: u64,
    msg: Option<Bytes>,
}

impl TryFrom<&mut Parse> for Ping {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping { command_size: parse.command_size(), msg: Some(msg) }),
            Err(parser::Error::EndOfStream) => Ok(Ping { command_size: parse.command_size(), msg: None }),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl Applicable for Ping {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        // If the destination is master connect, don't send PONG
        if !dst.db().role().await.is_master() && dst.writeable() {
            return Ok(());
        }
        let resp = match self.msg {
            None => Type::SimpleString("PONG".to_string()),
            Some(msg) => Type::BulkString(msg),
        };
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl Ping {
    pub fn new(command_size: u64, msg: Option<Bytes>) -> Ping {
        Ping { command_size, msg }
    }
}

impl Default for Ping {
    fn default() -> Self {
        Ping { command_size: 14, msg: None }
    }
}