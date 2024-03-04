use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use crate::{connection, resp};
use crate::db::DB;
use crate::encoder::Encoder;
use crate::parser::Parse;

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
    slaves: RwLock<HashMap<String, mpsc::Sender<Bytes>>>,
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

    pub async fn add_slave(&mut self, socket: String, tx: mpsc::Sender<Bytes>) {
        match &self.shard.role_type {
            Type::Master(info) => {
                let mut slaves = info.slaves.write().await;
                slaves.insert(socket, tx);
            }
            Type::Slave(_) => {}
        }
    }

    pub async fn delete_slave(&mut self, socket: &String) {
        match &self.shard.role_type {
            Type::Master(info) => {
                let mut slaves = info.slaves.write().await;
                slaves.remove(socket);
            }
            Type::Slave(_) => {}
        }
    }

    pub async fn replicate_data(&mut self, data: Bytes) {
        match &self.shard.role_type {
            Type::Master(info) => {
                let slaves = info.slaves.read().await;
                for (_, tx) in slaves.iter() {
                    let _ = tx.send(data.clone()).await;
                }
            }
            Type::Slave(_) => {}
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role {
            shard: Arc::new(
                Shard {
                    role_type: Type::Master(Master {
                        slaves: RwLock::new(HashMap::new()),
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

impl fmt::Display for Role {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "role:{}\nmaster_replid:{}\nmaster_repl_offset:{}", self.shard.role_type, self.shard.id, self.shard.offset.load(Ordering::Relaxed))
    }
}

pub async fn build_replica_connect(db: DB) -> crate::Result<()> {
    if let Some((ip, port)) = db.role().await.master_info() {
        let master_address = format!("{}:{}", ip, port);
        return match TcpStream::connect(master_address).await {
            Ok(stream) => {
                replica_connect(connection::Connection::new(stream, db, true)).await?;
                Ok(())
            }
            _ => {
                Err("connect master error".into())
            }
        };
    }
    Ok(())
}

async fn replica_connect(mut con: connection::Connection) -> crate::Result<()> {
    handshake_ping(&mut con).await?;
    handshake_replconf(&mut con).await?;
    handshake_psync(&mut con).await?;
    con.run().await
}

async fn handshake_ping(con: &mut connection::Connection) -> crate::Result<()> {
    let ping_order = resp::Type::Array(vec![
        resp::Type::BulkString("PING".into()),
    ]);
    con.write_all(Encoder::encode(&ping_order).as_slice()).await?;
    con.flush().await?;
    match con.read_frame().await {
        Ok(maybe_frame) => {
            let frame = maybe_frame.ok_or_else(|| "read frame error".to_string())?;
            let mut parse = Parse::new(frame)?;
            let info = parse.next_string()?.to_uppercase();
            if info != "PONG" {
                return Err("read frame error".into());
            }
            parse.finish()?;
            Ok(())
        }
        _ => Err("read frame error".into())
    }
}

async fn handshake_replconf(con: &mut connection::Connection) -> crate::Result<()> {
    let port = con.db().role().await.port().ok_or_else(|| "port not found".to_string())?;
    let port_order = resp::Type::Array(vec![
        resp::Type::BulkString("REPLCONF".into()),
        resp::Type::BulkString("listening-port".into()),
        resp::Type::BulkString(Bytes::from(port.to_string())),
    ]);
    con.write_all(Encoder::encode(&port_order).as_slice()).await?;
    con.flush().await?;
    match con.read_frame().await {
        Ok(maybe_frame) => {
            let frame = maybe_frame.ok_or_else(|| "read frame error".to_string())?;
            let mut parse = Parse::new(frame)?;
            let info = parse.next_string()?.to_uppercase();
            if info != "OK" {
                return Err("read frame error".into());
            }
            parse.finish()?;
        }
        _ => { return Err("read frame error".into()); }
    };
    let capa_order = resp::Type::Array(vec![
        resp::Type::BulkString("REPLCONF".into()),
        resp::Type::BulkString("capa".into()),
        resp::Type::BulkString("psync2".into()),
    ]);
    con.write_all(Encoder::encode(&capa_order).as_slice()).await?;
    con.flush().await?;
    match con.read_frame().await {
        Ok(maybe_frame) => {
            let frame = maybe_frame.ok_or_else(|| "read frame error".to_string())?;
            let mut parse = Parse::new(frame)?;
            let info = parse.next_string()?.to_uppercase();
            if info != "OK" {
                return Err("read frame error".into());
            }
            parse.finish()?;
            Ok(())
        }
        _ => { Err("read frame error".into()) }
    }
}

async fn handshake_psync(con: &mut connection::Connection) -> crate::Result<()> {
    let psync_order = resp::Type::Array(vec![
        resp::Type::BulkString("PSYNC".into()),
        resp::Type::BulkString("?".into()),
        resp::Type::BulkString("-1".into()),
    ]);
    con.write_all(Encoder::encode(&psync_order).as_slice()).await?;
    con.flush().await?;
    match con.read_frame().await {
        Ok(maybe_frame) => {
            let frame = maybe_frame.ok_or_else(|| "read frame error".to_string())?;
            let mut parse = Parse::new(frame)?;
            let info = parse.next_string()?.to_uppercase();
            if info != "FULLRESYNC" {
                return Err("read frame error".into());
            }
            let _replid = parse.next_string()?;
            let _offset = parse.next_int()?;
            parse.finish()?;
        }
        _ => { return Err("read frame error".into()); }
    }
    // receive rdb data
    match con.read_frame().await {
        Ok(maybe_frame) => {
            let frame = maybe_frame.ok_or_else(|| "read frame error".to_string())?;
            let mut parse = Parse::new(frame)?;
            let data = parse.next_bytes()?;
            con.db().write_rdb_data(&data).await?;
            Ok(())
        }
        _ => Err("read frame error".into())
    }
}
