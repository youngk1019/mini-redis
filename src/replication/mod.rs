use bytes::Bytes;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use crate::{connection, resp};
use crate::encoder::Encoder;
use crate::parser::Parse;

pub mod role;
pub mod command;
pub mod simple;
pub mod synchronization;

pub async fn replica_connect(con: &mut connection::Connection) -> crate::Result<()> {
    handshake_ping(con).await?;
    handshake_replconf(con).await?;
    handshake_psync(con).await?;
    Ok(())
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
            let mut parse = Parse::new(frame);
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
            let mut parse = Parse::new(frame);
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
            let mut parse = Parse::new(frame);
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
            let mut parse = Parse::new(frame);
            let info = parse.next_string()?.to_uppercase();
            // FULLRESYNC <REPL_ID> offset
            if !info.starts_with("FULLRESYNC") {
                return Err("read frame error".into());
            }
            parse.finish()?;
        }
        _ => { return Err("read frame error".into()); }
    }
    // receive rdb data
    match con.read_frame().await {
        Ok(maybe_frame) => {
            let frame = maybe_frame.ok_or_else(|| "read frame error".to_string())?;
            let mut parse = Parse::new(frame);
            let data = parse.next_bytes()?;
            con.db().write_rdb_data(&data).await?;
            parse.finish()?;
            Ok(())
        }
        _ => Err("read frame error".into())
    }
}

pub fn generate_id() -> String {
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