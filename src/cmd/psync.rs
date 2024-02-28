use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, Default, PartialEq)]
pub struct PSync {
    id: Option<String>,
    offset: Option<i64>,
}

impl TryFrom<&mut Parse> for PSync {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let mut id = None;
        let master_id = parse.next_string()?;
        if master_id.to_uppercase() != "?" {
            id = Some(master_id);
        }
        let offset = Some(parse.next_int()? as i64);
        Ok(PSync { id, offset })
    }
}

#[async_trait]
impl Applicable for PSync {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let role = dst.get_db().role();
        let mut resp = vec![Type::SimpleString("FULLRESYNC".to_string())];
        if let None = self.id {
            resp.push(Type::BulkString(Bytes::from(role.id())));
        }
        resp.push(Type::BulkString(Bytes::from(role.offset().to_string())));
        dst.write_all(Encoder::encode(&Type::Array(resp)).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}