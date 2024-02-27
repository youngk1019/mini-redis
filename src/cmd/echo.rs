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
    msg: Bytes,
}

impl TryFrom<&mut Parse> for Echo {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Echo::new(msg)),
            Err(parser::Error::EndOfStream) => Err("ECHO no message provided".into()),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl Applicable for Echo {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let resp = Type::BulkString(self.msg);
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl Echo {
    pub fn new(msg: Bytes) -> Echo {
        Echo { msg }
    }
}