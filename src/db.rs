use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::RwLock;
use crate::connection;
use crate::encoder::Encoder;
use crate::engine::{DataType, Engine, stream, string};
use crate::engine::stream::Entry;
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

    pub async fn get(&self, key: String) -> Result<Option<string::String>, Error> {
        let shard = self.shard.read().await;
        let val = shard.engine.get(key).await;
        match val {
            Some(DataType::String(string)) => Ok(Some(string)),
            None => Ok(None),
            _ => Err(Error::InvalidType),
        }
    }

    pub async fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut shard = self.shard.write().await;
        let val = string::String::new(value);
        shard.engine.set(key.clone(), DataType::String(val.clone()), expire).await;
        if shard.role.is_master() {
            let data = Encoder::encode(&Operation::Set(key, val).encode());
            shard.role.replicate_data(Command::Simple(Simple::new(data.into()))).await;
        }
    }

    pub async fn del(&mut self, keys: Vec<String>) -> u64 {
        let mut shard = self.shard.write().await;
        let mut count = 0u64;
        for key in keys.iter() {
            if shard.engine.del(key.clone()).await {
                count += 1;
            }
        }
        if shard.role.is_master() {
            let data = Encoder::encode(&Operation::Del(keys).encode());
            shard.role.replicate_data(Command::Simple(Simple::new(data.into()))).await;
        }
        return count;
    }

    pub async fn keys(&self) -> Vec<String> {
        let shard = self.shard.read().await;
        shard.engine.keys().await
    }

    pub async fn get_type(&self, key: String) -> &'static str {
        let shard = self.shard.read().await;
        let val = shard.engine.get(key).await;
        match val {
            Some(val) => val.type_name(),
            None => "none",
        }
    }

    pub async fn xadd(&self, key: String, id: Option<(u64, Option<u64>)>, fields: Vec<(Bytes, Bytes)>) -> Result<(u64, u64), Error> {
        let mut shard = self.shard.write().await;
        match shard.engine.get(key.clone()).await {
            Some(DataType::Stream(stream)) => {
                match stream.add_entry(id, fields.clone()).await {
                    Ok(id) => {
                        if shard.role.is_master() {
                            let data = Encoder::encode(&Operation::XAdd(key, Entry::new(id.0, id.1, fields)).encode());
                            shard.role.replicate_data(Command::Simple(Simple::new(data.into()))).await;
                        }
                        Ok(id)
                    }
                    Err(e) => Err(Error::StreamError(e)),
                }
            }
            None => {
                let stream = stream::Stream::new();
                let id = match stream.add_entry(id, fields.clone()).await {
                    Ok(id) => id,
                    Err(e) => return Err(Error::StreamError(e)),
                };
                shard.engine.set(key.clone(), DataType::Stream(stream), None).await;
                if shard.role.is_master() {
                    let data = Encoder::encode(&Operation::XAdd(key, Entry::new(id.0, id.1, fields)).encode());
                    shard.role.replicate_data(Command::Simple(Simple::new(data.into()))).await;
                }
                Ok(id)
            }
            _ => Err(Error::InvalidType),
        }
    }

    pub async fn xrange(&self, key: String, start: Option<(u64, Option<u64>)>, end: Option<(u64, Option<u64>)>, count: Option<u64>) -> Result<Vec<Entry>, Error> {
        let shard = self.shard.read().await;
        match shard.engine.get(key).await {
            Some(DataType::Stream(stream)) => {
                Ok(stream.range(start, end, count).await)
            }
            _ => Err(Error::InvalidType),
        }
    }

    pub async fn xread(&self, query: Vec<(String, (u64, Option<u64>))>, count: Option<u64>) -> Result<Vec<Vec<Entry>>, Error> {
        let shard = self.shard.read().await;
        let (keys, ids): (Vec<String>, Vec<(u64, Option<u64>)>) = query.into_iter().unzip();
        let mut streams = Vec::new();
        for key in keys.into_iter() {
            match shard.engine.get(key).await {
                Some(DataType::Stream(stream)) => {
                    streams.push(stream);
                }
                _ => return Err(Error::InvalidType),
            }
        }
        let mut entries = Vec::new();
        for (stream, id) in streams.into_iter().zip(ids.into_iter()) {
            entries.push(stream.range(Some(id), None, count).await);
        }
        Ok(entries)
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

enum Operation {
    Set(String, string::String),
    Del(Vec<String>),
    XAdd(String, Entry),
}

impl Operation {
    fn encode(self) -> Type {
        match self {
            Operation::Set(key, value) => {
                Type::Array(vec![
                    Type::BulkString("SET".into()),
                    Type::BulkString(key.into()),
                    value.encode(),
                ])
            }
            Operation::Del(key) => {
                let mut arr = vec![Type::BulkString("DEL".into())];
                for k in key.into_iter() {
                    arr.push(Type::BulkString(k.into()));
                }
                Type::Array(arr)
            }
            Operation::XAdd(key, entry) => {
                Type::Array(vec![
                    Type::BulkString("XADD".into()),
                    Type::BulkString(key.into()),
                    entry.encode(),
                ])
            }
        }
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidType,
    StreamError(stream::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::InvalidType => write!(f, "WRONGTYPE Operation against a key holding the wrong kind of value"),
            Error::StreamError(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}