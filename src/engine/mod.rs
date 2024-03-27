pub mod string;
pub mod stream;

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Notify, RwLock};
use tokio::{fs, time};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use crate::rdb::serializer::Serializer;
use crate::rdb::{self, types::Order};
use crate::rdb::parser::Parser;

#[derive(Debug, Clone)]
pub enum DataType {
    String(string::String),
    Stream(stream::Stream),
}

impl DataType {
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            DataType::String(_) => "string",
            DataType::Stream(_) => "stream",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    shard: Arc<Shard>,
}

#[derive(Debug)]
struct Shard {
    dir: String,
    file_name: String,
    path: PathBuf,
    kv: RwLock<KV>,
    background_task: Notify,
}

#[derive(Debug)]
struct KV {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    data: DataType,
    expiration: Option<Instant>,
}

impl Engine {
    pub(crate) async fn new(dir: String, file_name: String) -> Engine {
        let shard = Arc::new(Shard {
            dir: dir.clone(),
            file_name: file_name.clone(),
            path: PathBuf::from(dir).join(file_name),
            kv: RwLock::new({
                KV {
                    entries: HashMap::new(),
                    expirations: BTreeSet::new(),
                    shutdown: false,
                }
            }),
            background_task: Notify::new(),
        });
        let engine = Engine { shard: shard.clone() };
        if shard.path.exists() {
            engine.load_rdb().await.unwrap();
        }
        tokio::spawn(purge_expired_tasks(shard));
        engine
    }

    pub(crate) async fn get(&self, key: String) -> Option<DataType> {
        let kv = self.shard.kv.read().await;
        kv.entries.get(&key).map(|entry| entry.data.clone())
    }

    pub(crate) async fn set(&mut self, key: String, value: DataType, expire: Option<Duration>) {
        let mut kv = self.shard.kv.write().await;
        let mut notify = false;
        let expiration = expire.map(|duration| {
            let when = Instant::now() + duration;
            notify = kv
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            when
        });
        let prev = kv.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expiration,
            },
        );
        if let Some(prev) = prev {
            if let Some(when) = prev.expiration {
                // clear expiration
                kv.expirations.remove(&(when, key.clone()));
            }
        }
        if let Some(when) = expiration {
            kv.expirations.insert((when, key));
        }
        drop(kv);
        if notify {
            self.shard.background_task.notify_one();
        }
    }

    pub(crate) async fn del(&mut self, key: String) -> bool {
        let mut kv = self.shard.kv.write().await;
        if let Some(entry) = kv.entries.remove(&key) {
            if let Some(when) = entry.expiration {
                kv.expirations.remove(&(when, key));
            }
            true
        } else {
            false
        }
    }

    pub(crate) async fn keys(&self) -> Vec<String> {
        let kv = self.shard.kv.read().await;
        kv.entries.keys().cloned().collect()
    }

    pub(crate) async fn write_rdb(&self) -> crate::Result<()> {
        let kv = self.shard.kv.write().await;
        if self.shard.path.exists() {
            let bak = self.shard.path.with_extension("bak");
            fs::rename(&self.shard.path, &bak).await?;
        }
        let file = fs::File::create(&self.shard.path).await?;
        let mut serializer = Serializer::new(file);
        serializer.init().await?;
        for (key, entry) in kv.entries.iter() {
            let rtype = match &entry.data {
                DataType::String(str) => rdb::types::Type::String(key.clone().into(), str.clone().into()),
                DataType::Stream(_) => { continue; }
            };
            serializer.write_order(&Order {
                dataset: 0,
                rtype,
                expire: instant_to_system_time(entry.expiration),
            }).await?;
        }
        serializer.finish().await?;
        Ok(())
    }

    pub(crate) async fn write_rdb_data(&self, data: &[u8]) -> crate::Result<()> {
        let _kv = self.shard.kv.write().await;
        if self.shard.path.exists() {
            let bak = self.shard.path.with_extension("bak");
            fs::rename(&self.shard.path, &bak).await?;
        }
        let mut file = fs::File::create(&self.shard.path).await?;
        file.write_all(data).await?;
        Ok(())
    }

    pub(crate) async fn load_rdb(&self) -> crate::Result<()> {
        let mut kv = self.shard.kv.write().await;
        let file = fs::File::open(&self.shard.path).await?;
        let mut parser = Parser::new(file);
        parser.parse().await?;
        for order in parser.orders().map(|v| v.clone()) {
            let expiration = match system_time_to_instant(order.expire) {
                Ok(expiration) => expiration,
                Err(_) => continue,
            };
            let key = match order.rtype {
                rdb::types::Type::String(key, val) => {
                    kv.entries.insert(key.clone(), Entry {
                        data: DataType::String(string::String::new(val)),
                        expiration,
                    });
                    key
                }
                _ => continue,
            };
            if let Some(when) = expiration {
                kv.expirations.insert((when, key));
            }
        }
        drop(kv);
        self.shard.background_task.notify_one();
        Ok(())
    }

    pub fn dir(&self) -> String {
        self.shard.dir.clone()
    }

    pub fn file_name(&self) -> String {
        self.shard.file_name.clone()
    }

    pub(crate) async fn get_rdb(&self) -> crate::Result<Vec<u8>> {
        let _kv = self.shard.kv.write().await;
        match fs::read(&self.shard.path).await {
            Ok(data) => Ok(data),
            Err(e) => Err(e.into()),
        }
    }
}

impl Shard {
    async fn purge_expired_keys(&self) -> Option<Instant> {
        let kv = &mut *self.kv.write().await;
        if kv.shutdown {
            return None;
        }
        let now = Instant::now();
        while let Some(&(when, ref key)) = kv.expirations.iter().next() {
            if when > now {
                return Some(when);
            }
            kv.entries.remove(key);
            kv.expirations.remove(&(when, key.clone()));
        }
        None
    }

    async fn is_shutdown(&self) -> bool {
        self.kv.read().await.shutdown
    }
}

impl KV {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

async fn purge_expired_tasks(shard: Arc<Shard>) {
    while !shard.is_shutdown().await {
        if let Some(when) = shard.purge_expired_keys().await {
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shard.background_task.notified() => {}
            }
        } else {
            shard.background_task.notified().await;
        }
    }
}

fn instant_to_system_time(instant: Option<Instant>) -> Option<SystemTime> {
    instant.map(|instant| SystemTime::UNIX_EPOCH + instant.duration_since(Instant::now()))
}

fn system_time_to_instant(system_time: Option<SystemTime>) -> crate::Result<Option<Instant>> {
    match system_time {
        Some(system_time) => {
            match system_time.duration_since(SystemTime::now()) {
                Ok(duration) => {
                    Ok(Some(Instant::now() + duration))
                }
                Err(e) => Err(e.into())
            }
        }
        None => Ok(None)
    }
}
