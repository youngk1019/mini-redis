use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Echo {
    command_size: u64,
    msg: Bytes,
}

impl TryFrom<&mut Parse> for Echo {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Echo { command_size: parse.command_size(), msg }),
            Err(parser::Error::EndOfStream) => Err("ECHO no message provided".into()),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl Applicable for Echo {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let resp = Type::BulkString(self.msg);
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl Echo {
    pub fn new(command_size: u64, msg: Bytes) -> Echo {
        Echo { command_size, msg }
    }
}