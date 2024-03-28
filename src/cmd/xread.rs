use std::vec;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct XRead {
    command_size: u64,
    keys: Vec<String>,
    ids: Vec<Option<(u64, Option<u64>)>>,
    count: Option<u64>,
    block: Option<u64>,
}

impl TryFrom<&mut Parse> for XRead {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let (mut count, mut block) = (None, None);
        let mut key;
        loop {
            key = parse.next_string()?;
            match key.to_uppercase().as_str() {
                "COUNT" => count = Some(parse.next_int()?),
                "BLOCK" => block = Some(parse.next_int()?),
                _ => { break; }
            }
        }
        if key.to_uppercase() != "STREAMS" {
            return Err("Invalid XREAD format".into());
        }
        let (mut querys, mut keys, mut ids) = (Vec::new(), Vec::new(), Vec::new());
        loop {
            match parse.next_string() {
                Ok(k) => { querys.push(k); }
                Err(parser::Error::EndOfStream) => { break; }
                Err(err) => return Err(err.into()),
            }
        }
        if querys.len() % 2 != 0 {
            return Err("Invalid XREAD format".into());
        }
        for i in 0..querys.len() / 2 {
            keys.push(querys[i].clone());
        }
        for i in querys.len() / 2..querys.len() {
            if querys[i] == "$" {
                if block.is_none() {
                    return Err("Invalid XREAD format".into());
                }
                ids.push(None);
                continue;
            }
            let id_parts: Vec<&str> = querys[i].split('-').collect();
            if id_parts.len() == 1 {
                ids.push(Some((id_parts[0].parse().map_err(|_| "Invalid time format")?, None)));
            } else if id_parts.len() != 2 {
                return Err("Invalid ID format".into());
            } else {
                ids.push(Some((id_parts[0].parse().map_err(|_| "Invalid time format")?,
                               Some(id_parts[1].parse().map_err(|_| "Invalid seq format")?))));
            }
        }
        Ok(XRead { command_size: parse.command_size(), keys, ids, count, block })
    }
}

#[async_trait]
impl Applicable for XRead {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let resp = match dst.db().xread(self.keys.clone().into_iter().zip(self.ids).collect(), self.count, self.block).await {
            Ok(streams) => {
                let mut arr = Vec::new();
                for (stream, key) in streams.into_iter().zip(self.keys) {
                    if stream.is_empty() {
                        continue;
                    }
                    let mut stream_arr = Vec::new();
                    for entry in stream {
                        stream_arr.push(entry.encode());
                    }
                    arr.push(Type::Array(vec![Type::BulkString(key.into()), Type::Array(stream_arr)]));
                }
                if arr.is_empty() {
                    Type::Null
                } else {
                    Type::Array(arr)
                }
            }
            Err(e) => Type::SimpleError(e.to_string()),
        };
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}