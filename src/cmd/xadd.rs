use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct XAdd {
    command_size: u64,
    key: String,
    id: Option<(u64, Option<u64>)>,
    field: Vec<(Bytes, Bytes)>,
}

impl TryFrom<&mut Parse> for XAdd {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        let id_pattern = parse.next_string()?;
        let id;
        if id_pattern == "*" {
            id = None;
        } else {
            let id_parts: Vec<&str> = id_pattern.split('-').collect();
            if id_parts.len() != 2 {
                return Err("Invalid ID format".into());
            }
            let seq = if id_parts[1] == "*" {
                None
            } else {
                Some(id_parts[1].parse().map_err(|_| "Invalid seq format")?)
            };
            id = Some((id_parts[0].parse().map_err(|_| "Invalid time format")?, seq));
        }
        let mut field = Vec::new();
        loop {
            match parse.next_bytes() {
                Ok(key) => {
                    let val = parse.next_bytes()?;
                    field.push((key, val));
                }
                Err(parser::Error::EndOfStream) => { break; }
                Err(err) => return Err(err.into()),
            }
        }
        Ok(XAdd { command_size: parse.command_size(), key, id, field })
    }
}

#[async_trait]
impl Applicable for XAdd {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if !dst.writeable() {
            return Ok(());
        }
        dst.db().role().await.add_offset(self.command_size);
        let resp = match dst.db().xadd(self.key, self.id, self.field).await {
            Ok((time, seq)) => Type::SimpleString(format!("{}-{}", time, seq)),
            Err(e) => Type::SimpleError(e.to_string()),
        };
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}