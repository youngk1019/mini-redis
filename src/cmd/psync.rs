use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, Default, PartialEq)]
pub struct PSync {
    id: Option<String>,
    offset: Option<u64>,
}

impl TryFrom<&mut Parse> for PSync {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let mut id = None;
        let mut offset = None;
        let master_id = parse.next_string()?;
        if master_id.to_uppercase() != "?" {
            id = Some(master_id);
        }
        match parse.next_int() {
            Ok(o) => { offset = Some(o); }
            _ => {}
        }
        Ok(PSync { id, offset })
    }
}

#[async_trait]
impl Applicable for PSync {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let role = dst.get_db().role();
        let resp = Type::SimpleString(format!("FULLRESYNC {} {}", role.id(), self.offset.unwrap_or(0)).into());
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}