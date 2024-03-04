use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::Mutex;
use crate::connection;
use crate::encoder::Encoder;
use crate::engine::Engine;
use crate::replication::Role;
use crate::resp::Type;

#[derive(Debug, Clone)]
pub struct DB {
    shard: Arc<Mutex<Shard>>,
}

#[derive(Debug)]
struct Shard {
    engine: Engine,
    role: Role,
}

impl DB {
    pub fn new(path: PathBuf, role: Option<Role>) -> DB {
        let engine = Engine::new(path);
        let role = role.unwrap_or_default();
        DB {
            shard: Arc::new(Mutex::new(Shard {
                engine,
                role,
            }))
        }
    }

    pub async fn get(&self, key: String) -> Option<Bytes> {
        let shard = self.shard.lock().await;
        shard.engine.get(key).await
    }

    pub async fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut shard = self.shard.lock().await;
        shard.engine.set(key.clone(), value.clone(), expire).await;
        let data = Encoder::encode(&Type::Array(vec![
            Type::BulkString("SET".into()),
            Type::BulkString(key.into()),
            Type::BulkString(value),
        ]));
        shard.role.add_offset(data.len() as u64);
        if shard.role.is_master() {
            shard.role.replicate_data(data.into()).await;
        }
    }

    #[allow(dead_code)]
    pub async fn del(&mut self, key: String) -> bool {
        let mut shard = self.shard.lock().await;
        shard.engine.del(key).await
    }

    #[allow(dead_code)]
    pub async fn keys(&self) -> Vec<String> {
        let shard = self.shard.lock().await;
        shard.engine.keys().await
    }

    pub async fn rdb_sync(&self) -> crate::Result<()> {
        let mut shard = self.shard.lock().await;
        shard.engine.write_rdb().await?;
        shard.role.set_offset(0);
        Ok(())
    }

    pub async fn write_rdb_data(&self, data: &[u8]) -> crate::Result<()> {
        let mut shard = self.shard.lock().await;
        shard.engine.write_rdb_data(data).await?;
        shard.engine.load_rdb().await?;
        shard.role.set_offset(0);
        Ok(())
    }

    pub async fn read_rdb(&self) -> crate::Result<Vec<u8>> {
        let mut shard = self.shard.lock().await;
        shard.engine.write_rdb().await?;
        shard.role.set_offset(0);
        shard.engine.get_rdb().await
    }

    pub async fn role(&self) -> Role {
        let shard = self.shard.lock().await;
        shard.role.clone()
    }

    pub async fn add_slave(&self, key: String, con: &mut connection::Connection) -> crate::Result<Receiver<Bytes>> {
        let mut shard = self.shard.lock().await;
        // send RDB file to slave
        shard.engine.write_rdb().await?;
        shard.role.set_offset(0);
        let data = shard.engine.get_rdb().await?;
        let resp = Type::RDBFile(data.into());
        con.write_all(Encoder::encode(&resp).as_slice()).await?;
        con.flush().await?;
        // add slave to master
        let (tx, rx) = channel::<Bytes>(32);
        shard.role.add_slave(key, tx).await;
        Ok(rx)
    }

    pub async fn delete_slave(&self, key: &String) {
        let mut shard = self.shard.lock().await;
        shard.role.delete_slave(key).await;
    }
}


