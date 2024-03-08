use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct DEL {
    command_size: u64,
    keys: Vec<String>,
}

impl TryFrom<&mut Parse> for DEL {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let mut keys = Vec::new();
        while let Ok(key) = parse.next_string() {
            keys.push(key);
        }
        Ok(DEL { command_size: parse.command_size(), keys })
    }
}

#[async_trait]
impl Applicable for DEL {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let count = dst.db().del(self.keys).await;
        dst.write_all(Encoder::encode(&Type::Integer(count)).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}