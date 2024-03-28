use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Info {
    command_size: u64,
    info: InfoType,
}

#[derive(Debug, PartialEq)]
pub enum InfoType {
    Replication,
}

impl TryFrom<&mut Parse> for Info {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let req = parse.next_string()?;
        let info = match req.to_uppercase().as_str() {
            "REPLICATION" => InfoType::Replication,
            _ => return Err("INFO only supports `REPLICATION`".into()),
        };
        Ok(Info { command_size: parse.command_size(), info })
    }
}

#[async_trait]
impl Applicable for Info {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        match self.info {
            InfoType::Replication => {
                let role = dst.db().role().await;
                let resp = Type::BulkString(Bytes::from(format!("{}", role).into_bytes()));
                if dst.need_update_offset().await {
                    dst.db().role().await.add_offset(self.command_size);
                }
                dst.write_all(Encoder::encode(&resp).as_slice()).await?;
                dst.flush().await?;
            }
        }
        Ok(())
    }
}

impl Info {
    pub fn new(command_size: u64,info: InfoType) -> Info {
        Info {command_size, info }
    }
}