use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
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
        let expire = None;
        Ok(Set { key, value, expire })
    }
}

#[async_trait]
impl Applicable for Set {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        dst.get_db().set(self.key, self.value, self.expire);
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
