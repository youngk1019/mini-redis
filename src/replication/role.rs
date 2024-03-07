use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, Mutex};
use super::command::Command;
use super::generate_id;

#[derive(Debug, Clone)]
pub struct Role {
    shard: Arc<Shard>,
}

#[derive(Debug)]
struct Shard {
    role_type: Type,
    id: String,
    offset: AtomicU64,
}

#[derive(Debug)]
enum Type {
    Master(Master),
    Slave(Slave),
}

#[derive(Debug)]
struct Slave {
    port: usize,
    master_ip: String,
    master_port: usize,
}

#[derive(Debug)]
struct Master {
    // TODO: remove mutex because hashmap will only be used when DB is locked
    slaves: Mutex<HashMap<String, mpsc::Sender<Command>>>,
}

impl Role {
    pub fn new_slave(port: usize, master_ip: String, master_port: usize) -> Role {
        Role {
            shard: Arc::new(Shard {
                role_type: Type::Slave(Slave {
                    port,
                    master_ip,
                    master_port,
                }),
                id: generate_id(),
                offset: AtomicU64::new(0),
            })
        }
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.shard.offset.store(offset, Ordering::Relaxed);
    }

    pub fn add_offset(&mut self, offset: u64) {
        self.shard.offset.fetch_add(offset, Ordering::Relaxed);
    }

    pub fn offset(&self) -> u64 {
        self.shard.offset.load(Ordering::Relaxed)
    }

    pub fn master_info(&self) -> Option<(String, usize)> {
        match &self.shard.role_type {
            Type::Master(_) => None,
            Type::Slave(info) => Some((info.master_ip.clone(), info.master_port)),
        }
    }

    pub fn port(&self) -> Option<usize> {
        match &self.shard.role_type {
            Type::Master(_) => None,
            Type::Slave(info) => Some(info.port),
        }
    }

    pub fn id(&self) -> String {
        self.shard.id.clone()
    }

    pub fn is_master(&self) -> bool {
        match &self.shard.role_type {
            Type::Master(_) => true,
            Type::Slave(_) => false,
        }
    }

    pub async fn add_slave(&mut self, key: String, tx: mpsc::Sender<Command>) {
        match &self.shard.role_type {
            Type::Master(info) => {
                let mut slaves = info.slaves.lock().await;
                slaves.insert(key, tx);
            }
            Type::Slave(_) => {}
        }
    }

    pub async fn delete_slave(&mut self, key: &String) {
        match &self.shard.role_type {
            Type::Master(info) => {
                let mut slaves = info.slaves.lock().await;
                slaves.remove(key);
            }
            Type::Slave(_) => {}
        }
    }

    pub async fn replicate_data(&mut self, data: Command) {
        match &self.shard.role_type {
            Type::Master(info) => {
                let slaves = info.slaves.lock().await;
                for (_, tx) in slaves.iter() {
                    let _ = tx.send(data.clone()).await;
                }
            }
            Type::Slave(_) => {}
        }
    }

    pub async fn slave_count(&self) -> u64 {
        match &self.shard.role_type {
            Type::Master(info) => {
                let slaves = info.slaves.lock().await;
                slaves.len() as u64
            }
            Type::Slave(_) => 0,
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role {
            shard: Arc::new(
                Shard {
                    role_type: Type::Master(Master {
                        slaves: Mutex::new(HashMap::new()),
                    }),
                    id: generate_id(),
                    offset: AtomicU64::new(0),
                }
            )
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let repr = match self {
            Type::Master(_) => "master",
            Type::Slave(_) => "slave",
        };
        fmt.write_str(repr)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}", self.shard.role_type, self.shard.id, self.shard.offset.load(Ordering::Relaxed))
    }
}
