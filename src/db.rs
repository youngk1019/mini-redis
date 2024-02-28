use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use bytes::Bytes;
use tokio::sync::Notify;
use tokio::time;
use tokio::time::Instant;

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
    id: String,
    offset: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ReplicationType {
    Master,
    Slave,
}

impl DB {
    pub fn new() -> DB {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                role: Role::default(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });
        tokio::spawn(purge_expired_tasks(shared.clone()));
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
        let state = self.shared.state.lock().unwrap();
        state.role.clone()
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

impl Default for Role {
    fn default() -> Self {
        Role {
            role: ReplicationType::Master,
            id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            offset: 0,
        }
    }
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
        write!(fmt, "role:{}", self.role)
    }
}