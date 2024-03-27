use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;
use bytes::Bytes;
use tokio::sync::RwLock;
use crate::resp;

#[derive(Debug, Clone)]
pub struct Stream {
    shard: Arc<Shard>,
}

#[derive(Debug)]
struct Shard {
    entries: RwLock<BTreeMap<(u128, u64), Vec<Bytes>>>,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            shard: Arc::new(Shard {
                entries: RwLock::new(BTreeMap::new()),
            }),
        }
    }

    pub async fn add_entry(&self, id: (Option<u128>, Option<u64>), fields: Vec<Bytes>) -> Result<(u128, u64), Error> {
        let mut shard = self.shard.entries.write().await;
        let (last_time, last_seq) = shard.iter().last().map(|(k, _)| k).unwrap_or(&(0, 0)).clone();
        let time = id.0.unwrap_or(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis());
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
        shard.insert((time, seq), fields);
        Ok((time, seq))
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

pub struct Entry {
    time: u128,
    seq: u64,
    fields: Vec<Bytes>,
}

impl Entry {
    pub fn new(time: u128, seq: u64, fields: Vec<Bytes>) -> Self {
        Entry { time, seq, fields }
    }

    pub fn encode(&self) -> resp::Type {
        let mut entry = Vec::new();
        entry.push(resp::Type::BulkString(format!("{}-{}", self.time, self.seq).into()));
        let mut fields = Vec::new();
        for v in self.fields.iter() {
            fields.push(resp::Type::BulkString(v.clone()));
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