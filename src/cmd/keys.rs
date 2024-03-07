use async_trait::async_trait;
use regex::Regex;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug)]
pub struct Keys {
    command_size: u64,
    regex: Regex,
}

impl TryFrom<&mut Parse> for Keys {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let ask = parse.next_string()?
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".")
            .replace("[", "\\[")
            .replace("]", "\\]");
        let regex = match Regex::new(&ask) {
            Ok(r) => r,
            Err(e) => {
                return Err(e.into());
            }
        };
        Ok(Keys { command_size: parse.command_size(), regex })
    }
}

#[async_trait]
impl Applicable for Keys {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let mut resp = vec![];
        for key in dst.db().keys().await {
            if self.regex.is_match(&key) {
                resp.push(Type::BulkString(key.into()));
            }
        }
        dst.write_all(Encoder::encode(&Type::Array(resp)).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}

impl PartialEq for Keys {
    fn eq(&self, other: &Self) -> bool {
        self.regex.as_str() == other.regex.as_str()
    }
}