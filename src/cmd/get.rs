use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Get {
    command_size: u64,
    key: String,
}

impl TryFrom<&mut Parse> for Get {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        Ok(Get { command_size: parse.command_size(), key })
    }
}

#[async_trait]
impl Applicable for Get {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let resp = match dst.db().get(self.key).await {
            Ok(Some(value)) => value.encode(),
            Ok(None) => Type::Null,
            Err(e) => Type::SimpleError(e.to_string()),
        };
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl Get {
    pub fn new(command_size: u64,key: impl ToString) -> Get {
        Get {
            command_size,
            key: key.to_string(),
        }
    }
}