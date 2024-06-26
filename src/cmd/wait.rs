use std::cmp::min;
use std::time::Duration;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::replication::synchronization::Synchronization;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Wait {
    command_size: u64,
    num_replicas: u64,
    timeout: Option<Duration>,
}

impl TryFrom<&mut Parse> for Wait {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let num_replicas = parse.next_int()?;
        let timeout = parse.next_int()?;
        let timeout = if timeout == 0 { None } else { Some(Duration::from_millis(timeout)) };
        Ok(Wait { command_size: parse.command_size(), num_replicas, timeout })
    }
}

#[async_trait]
impl Applicable for Wait {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let mut sync = Synchronization::new(self.timeout, min(dst.db().slave_count().await, self.num_replicas));
        dst.db().sync_replication(sync.clone()).await;
        sync.wait().await;
        let resp = Type::Integer(sync.have_finish());
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}