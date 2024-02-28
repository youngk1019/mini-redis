use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Info {
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
        Ok(Info { info })
    }
}

#[async_trait]
impl Applicable for Info {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        match self.info {
            InfoType::Replication => {
                let role = dst.get_db().role();
                let resp = Type::BulkString(Bytes::from(format!("{}", role).into_bytes()));
                dst.write_all(Encoder::encode(&resp).as_slice()).await?;
                dst.flush().await?;
            }
        }
        Ok(())
    }
}

impl Info {
    pub fn new(info: InfoType) -> Info {
        Info { info }
    }
}