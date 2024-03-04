use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tokio::sync::Mutex;
use crate::engine::Engine;
use crate::replication::Role;

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
        shard.engine.set(key, value, expire).await;
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
}


