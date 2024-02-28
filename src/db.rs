use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use bytes::Bytes;
use tokio::sync::Notify;
use tokio::time;
use tokio::time::Instant;
use rand::{distributions::Alphanumeric, Rng};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use crate::encoder::Encoder;
use crate::resp;

#[derive(Debug, Clone)]
pub struct DB {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
    role: Role,
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
    expiration: Option<Instant>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Role {
    role: ReplicationType,
    master_ip: Option<String>,
    master_port: Option<usize>,
    id: String,
    offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum ReplicationType {
    Master,
    Slave,
}

impl DB {
    pub fn new(role: Option<Role>) -> DB {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                role: role.unwrap_or_default(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });
        tokio::spawn(purge_expired_tasks(shared.clone()));
        if shared.role().role == ReplicationType::Slave {
            tokio::spawn(replica_connect(shared.clone()));
        }
        DB { shared }
    }

    pub(crate) fn get(&self, key: String) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(&key).map(|entry| entry.data.clone())
    }

    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        let mut notify = false;
        let expiration = expire.map(|duration| {
            let when = Instant::now() + duration;
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            when
        });
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expiration,
            },
        );
        if let Some(prev) = prev {
            if let Some(when) = prev.expiration {
                // clear expiration
                state.expirations.remove(&(when, key.clone()));
            }
        }
        if let Some(when) = expiration {
            state.expirations.insert((when, key));
        }

        drop(state);
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn del(&self, key: String) -> bool {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(entry) = state.entries.remove(&key) {
            if let Some(when) = entry.expiration {
                state.expirations.remove(&(when, key));
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn role(&self) -> Role {
        self.shared.role()
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

impl Shared {
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return None;
        }
        let state = &mut *state;
        let now = Instant::now();
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }
        None
    }

    fn role(&self) -> Role {
        self.state.lock().unwrap().role.clone()
    }

    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
}

impl Role {
    pub fn new(ip: Option<String>, port: Option<usize>) -> Role {
        Role {
            role: ReplicationType::Slave,
            master_ip: ip,
            master_port: port,
            id: generate_id(),
            offset: 0,
        }
    }

    pub fn master_info(&self) -> Option<(String, usize)> {
        match self.role {
            ReplicationType::Master => None,
            ReplicationType::Slave => Some((self.master_ip.clone()?, self.master_port?)),
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role {
            role: ReplicationType::Master,
            master_ip: None,
            master_port: None,
            id: generate_id(),
            offset: 0,
        }
    }
}

fn generate_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .filter_map(|b| {
            let c = b as char;
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                Some(c)
            } else {
                None
            }
        })
        .take(40)
        .collect()
}

impl fmt::Display for ReplicationType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let repr = match self {
            ReplicationType::Master => "master",
            ReplicationType::Slave => "slave",
        };
        fmt.write_str(repr)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}", self.role, self.id, self.offset)
    }
}

async fn replica_connect(shared: Arc<Shared>) {
    let ping_order = resp::Type::Array(vec![
        resp::Type::BulkString("PING".into()),
    ]);
    let role = shared.role();
    if let Some((ip, port)) = role.master_info() {
        let master_address = format!("{}:{}", ip, port);
        match TcpStream::connect(master_address).await {
            Ok(mut stream) => {
                let _ = stream.write_all(Encoder::encode(&ping_order).as_slice()).await;
            }
            _ => {}
        }
    }
}