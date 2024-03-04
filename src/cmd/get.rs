use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Get {
    key: String,
}

impl TryFrom<&mut Parse> for Get {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }
}

#[async_trait]
impl Applicable for Get {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let resp = match dst.db().get(self.key).await {
            Some(value) => Type::BulkString(value),
            None => Type::Null,
        };
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl Get {
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }
}