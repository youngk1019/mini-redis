use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::{self, Parse};
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl TryFrom<&mut Parse> for Set {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        let mut expire = None;
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(parser::Error::EndOfStream) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(Set { key, value, expire })
    }
}

#[async_trait]
impl Applicable for Set {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if !dst.writeable() {
            return Ok(());
        }
        dst.db().set(self.key, self.value, self.expire).await;
        let resp = Type::SimpleString("OK".to_string());
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }
}
