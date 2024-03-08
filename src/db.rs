use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::RwLock;
use crate::connection;
use crate::encoder::Encoder;
use crate::engine::Engine;
use crate::replication::command::Command;
use crate::replication::role::Role;
use crate::replication::simple::Simple;
use crate::replication::synchronization::Synchronization;
use crate::resp::Type;

#[derive(Debug, Clone)]
pub struct DB {
    shard: Arc<RwLock<Shard>>,
}

#[derive(Debug)]
struct Shard {
    engine: Engine,
    role: Role,
}

impl DB {
    pub async fn new(dir: String, file_name: String, role: Option<Role>) -> DB {
        let engine = Engine::new(dir, file_name).await;
        let role = role.unwrap_or_default();
        DB {
            shard: Arc::new(RwLock::new(Shard {
                engine,
                role,
            }))
        }
    }

    pub async fn get(&self, key: String) -> Option<Bytes> {
        let shard = self.shard.read().await;
        shard.engine.get(key).await
    }

    pub async fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut shard = self.shard.write().await;
        shard.engine.set(key.clone(), value.clone(), expire).await;
        if shard.role.is_master() {
            let data = Encoder::encode(&Type::Array(vec![
                Type::BulkString("SET".into()),
                Type::BulkString(key.into()),
                Type::BulkString(value),
            ]));
            shard.role.replicate_data(Command::Simple(Simple::new(data.into()))).await;
        }
    }

    pub async fn del(&mut self, keys: Vec<String>) -> u64 {
        let mut shard = self.shard.write().await;
        let mut count = 0u64;
        for key in keys.into_iter() {
            if shard.engine.del(key).await {
                count += 1;
            }
        }
        return count;
    }

    pub async fn keys(&self) -> Vec<String> {
        let shard = self.shard.read().await;
        shard.engine.keys().await
    }

    pub async fn rdb_sync(&self) -> crate::Result<()> {
        let mut shard = self.shard.write().await;
        shard.engine.write_rdb().await?;
        shard.role.set_offset(0);
        Ok(())
    }

    pub async fn write_rdb_data(&self, data: &[u8]) -> crate::Result<()> {
        let mut shard = self.shard.write().await;
        shard.engine.write_rdb_data(data).await?;
        shard.engine.load_rdb().await?;
        shard.role.set_offset(0);
        Ok(())
    }

    pub async fn read_rdb(&self) -> crate::Result<Vec<u8>> {
        let mut shard = self.shard.write().await;
        shard.engine.write_rdb().await?;
        shard.role.set_offset(0);
        shard.engine.get_rdb().await
    }

    pub async fn role(&self) -> Role {
        let shard = self.shard.read().await;
        shard.role.clone()
    }

    pub async fn add_slave(&self, key: String, con: &mut connection::Connection) -> crate::Result<Receiver<Command>> {
        let mut shard = self.shard.write().await;
        // send RDB file to slave
        shard.engine.write_rdb().await?;
        shard.role.set_offset(0);
        let data = shard.engine.get_rdb().await?;
        let resp = Type::RDBFile(data.into());
        con.write_all(Encoder::encode(&resp).as_slice()).await?;
        con.flush().await?;
        // add slave to master
        let (tx, rx) = channel::<Command>(32);
        shard.role.add_slave(key, tx).await;
        Ok(rx)
    }

    pub async fn delete_slave(&self, key: &String) {
        let mut shard = self.shard.write().await;
        shard.role.delete_slave(key).await;
    }

    pub async fn slave_count(&self) -> u64 {
        let shard = self.shard.read().await;
        shard.role.slave_count().await
    }

    pub async fn sync_replication(&self, sync: Synchronization) {
        let mut shard = self.shard.write().await;
        shard.role.replicate_data(Command::Synchronization(sync)).await;
    }

    pub async fn dir(&self) -> String {
        let shard = self.shard.read().await;
        shard.engine.dir()
    }

    pub async fn file_name(&self) -> String {
        let shard = self.shard.read().await;
        shard.engine.file_name()
    }
}


