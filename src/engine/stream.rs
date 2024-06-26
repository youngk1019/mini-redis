use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;
use bytes::Bytes;
use tokio::sync::{mpsc, RwLock};
use crate::{resp, utils};

#[derive(Debug, Clone)]
pub struct Stream {
    shard: Arc<Shard>,
}

#[derive(Debug)]
struct Shard {
    entries: RwLock<BTreeMap<(u64, u64), Vec<(Bytes, Bytes)>>>,
    listener: RwLock<HashMap<String, mpsc::Sender<Entry>>>,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            shard: Arc::new(Shard {
                entries: RwLock::new(BTreeMap::new()),
                listener: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub async fn add_entry(&self, id: Option<(u64, Option<u64>)>, fields: Vec<(Bytes, Bytes)>) -> Result<(u64, u64), Error> {
        let mut shard = self.shard.entries.write().await;
        let (last_time, last_seq) = shard.iter().last().map(|(k, _)| k).unwrap_or(&(0, 0)).clone();
        let id = id.unwrap_or((SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64, None));
        let time = id.0;
        let seq = id.1.unwrap_or(if time <= last_time { last_seq + 1 } else { 0 });
        if time == 0 && seq == 0 {
            return Err(Error::ZeroID);
        }
        if time < last_time {
            return Err(Error::InvalidID);
        }
        if time <= last_time && seq <= last_seq {
            return Err(Error::InvalidID);
        }
        shard.insert((time, seq), fields.clone());
        let entry = Entry::new(time, seq, fields);
        let listeners = self.shard.listener.read().await;
        for listener in listeners.iter() {
            let _ = listener.1.send(entry.clone()).await;
        }
        Ok((time, seq))
    }

    pub async fn range(&self, start: Option<(u64, Option<u64>)>, end: Option<(u64, Option<u64>)>, count: Option<u64>) -> Vec<Entry> {
        let shard = self.shard.entries.read().await;
        let mut entries = Vec::new();
        let start = start.unwrap_or((0, None));
        let start = (start.0, start.1.unwrap_or(0));
        let end = end.unwrap_or((u64::MAX, None));
        let end = (end.0, end.1.unwrap_or(u64::MAX));
        let mut iter = shard.range(start..=end);
        while let Some(((time, seq), fields)) = iter.next() {
            entries.push(Entry::new(*time, *seq, fields.clone()));
            if let Some(c) = count {
                if entries.len() >= c as usize {
                    break;
                }
            }
        }
        entries
    }

    pub async fn listen(&self) -> (String, mpsc::Receiver<Entry>) {
        let (tx, rx) = mpsc::channel(32);
        let mut listeners = self.shard.listener.write().await;
        let mut key = utils::strings::generate_id(None);
        while listeners.contains_key(&key) {
            key = utils::strings::generate_id(None);
        }
        listeners.insert(key.clone(), tx);
        (key, rx)
    }

    pub async fn unlisten(&self, key: String) {
        let mut listeners = self.shard.listener.write().await;
        listeners.remove(&key);
    }

    pub async fn encode(&self) -> resp::Type {
        let shard = self.shard.entries.read().await;
        let mut entries = Vec::new();
        for ((time, seq), fields) in shard.iter() {
            entries.push(Entry::new(*time, *seq, fields.clone()).encode());
        }
        resp::Type::Array(entries)
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    time: u64,
    seq: u64,
    fields: Vec<(Bytes, Bytes)>,
}

impl Entry {
    pub fn new(time: u64, seq: u64, fields: Vec<(Bytes, Bytes)>) -> Self {
        Entry { time, seq, fields }
    }

    pub fn encode(self) -> resp::Type {
        let mut entry = Vec::new();
        entry.push(resp::Type::BulkString(format!("{}-{}", self.time, self.seq).into()));
        let mut fields = Vec::new();
        for v in self.fields.into_iter() {
            fields.push(resp::Type::BulkString(v.0));
            fields.push(resp::Type::BulkString(v.1));
        }
        entry.push(resp::Type::Array(fields));
        resp::Type::Array(entry)
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidID,
    ZeroID,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::InvalidID => write!(f, "ERR The ID specified in XADD is equal or smaller than the target stream top item"),
            Error::ZeroID => write!(f, "ERR The ID specified in XADD must be greater than 0-0"),
        }
    }
}

impl std::error::Error for Error {}