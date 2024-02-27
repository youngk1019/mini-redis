use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::parser::{self, Parse};
use crate::resp::Type;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;

#[derive(Debug, Default, PartialEq)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl TryFrom<&mut Parse> for Ping {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(parser::Error::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl Applicable for Ping {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
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
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }
}