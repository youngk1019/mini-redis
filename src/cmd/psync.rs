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
        if let Some(socket) = dst.socket_addr() {
            let role = dst.db().role().await;
            let resp = Type::SimpleString(format!("FULLRESYNC {} {}", role.id(), role.offset()));
            dst.write_all(Encoder::encode(&resp).as_slice()).await?;
            let db = dst.db().clone();
            let mut rx = db.add_slave(socket.clone(), dst).await?;
            let sender = async {
                while let Some(data) = rx.recv().await {
                    dst.write_all(data.as_ref()).await?;
                    dst.flush().await?;
                }
                Ok(())
            };
            let _: crate::Result<()> = sender.await;
            dst.db().delete_slave(&socket).await;
        }
        Ok(())
    }
}