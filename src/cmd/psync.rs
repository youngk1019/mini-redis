use std::cmp::max;
use std::time::Duration;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use crate::cmd;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::replication::command::Command;
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
            let key = socket + &*dst.id();
            let (mut now_offset, mut need_offset) = (0u64, 0u64);
            let resp = Type::SimpleString(format!("FULLRESYNC {} {}", role.id(), now_offset));
            dst.write_all(Encoder::encode(&resp).as_slice()).await?;
            let db = dst.db().clone();
            let mut rx = db.add_slave(key.clone(), dst).await?;
            let sender = async {
                loop {
                    tokio::select! {
                        cmd = rx.recv() => {
                            if let Some(cmd) = cmd {
                                match cmd {
                                    Command::Simple(simple) => {
                                        need_offset += simple.data().len() as u64;
                                        dst.write_all(simple.data()).await?;
                                        dst.flush().await?
                                    }
                                    Command::Synchronization(sync) => {
                                        let timeout = sync.timeout();
                                        let now = Instant::now();
                                        while need_offset > now_offset {
                                            need_offset += 37; // len of "REPLCONF GETACK *"
                                            now_offset = max(now_offset, update_offset(dst, timeout).await?);
                                            if let Some(timeout) = timeout {
                                                if now.elapsed() > timeout {
                                                    break;
                                                }
                                            }
                                        }
                                        if need_offset <= now_offset{
                                            sync.finish();
                                        }
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                        _ = dst.read_frame() => {
                            // nothing need to do
                        }
                    }
                }
                Ok(())
            };
            let _: crate::Result<()> = sender.await;
            dst.db().delete_slave(&key).await;
        }
        Ok(())
    }
}

pub async fn update_offset(dst: &mut Connection, timeout: Option<Duration>) -> crate::Result<u64> {
    let mut offset = 0u64;
    let req = Type::Array(vec![
        Type::BulkString("REPLCONF".into()),
        Type::BulkString("GETACK".into()),
        Type::BulkString("*".into()),
    ]);
    dst.write_all(Encoder::encode(&req).as_slice()).await?;
    dst.flush().await?;
    tokio::select! {
        cmd = dst.read_frame() => {
            if let Ok(Some(cmd)) = cmd {
                if let Ok(cmd) = cmd::Command::try_from(cmd) {
                    if let cmd::Command::ReplConf(replconf) = cmd {
                        offset = replconf.offset();
                    }
                }
            }
        }
        _ = tokio::time::sleep(timeout.unwrap_or(Duration::from_secs(u64::MAX))) => {}
    }
    Ok(offset)
}