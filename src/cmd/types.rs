use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp;

#[derive(Debug, PartialEq)]
pub struct Type {
    command_size: u64,
    key: String,
}

impl TryFrom<&mut Parse> for Type {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        Ok(Type { command_size: parse.command_size(), key })
    }
}

#[async_trait]
impl Applicable for Type {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let resp = match dst.db().get(self.key).await {
            Some(value) => resp::Type::SimpleString(value.type_name().into()),
            None => resp::Type::SimpleString("none".into()),
        };
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl Type {
    pub fn new(key: impl ToString) -> Type {
        Type {
            command_size: 0,
            key: key.to_string(),
        }
    }
}