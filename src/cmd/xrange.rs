use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct XRange {
    command_size: u64,
    key: String,
    start: Option<(u64, Option<u64>)>,
    end: Option<(u64, Option<u64>)>,
    count: Option<u64>,
}

impl TryFrom<&mut Parse> for XRange {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let key = parse.next_string()?;
        let start_pattern = parse.next_string()?;
        let start;
        if start_pattern == "-" {
            start = None;
        } else {
            let start_parts: Vec<&str> = start_pattern.split('-').collect();
            if start_parts.len() == 1 {
                start = Some((start_parts[0].parse().map_err(|_| "Invalid time format")?, None));
            } else if start_parts.len() != 2 {
                return Err("Invalid start format".into());
            } else {
                start = Some((start_parts[0].parse().map_err(|_| "Invalid time format")?,
                              Some(start_parts[1].parse().map_err(|_| "Invalid time format")?)));
            }
        }
        let end_pattern = parse.next_string()?;
        let end;
        if end_pattern == "+" {
            end = None;
        } else {
            let end_parts: Vec<&str> = end_pattern.split('-').collect();
            if end_parts.len() == 1 {
                end = Some((end_parts[0].parse().map_err(|_| "Invalid time format")?, None));
            } else if end_parts.len() != 2 {
                return Err("Invalid end format".into());
            } else {
                end = Some((end_parts[0].parse().map_err(|_| "Invalid time format")?,
                            Some(end_parts[1].parse().map_err(|_| "Invalid time format")?)));
            }
        }
        let count = match parse.next_string() {
            Ok(c) => {
                if c.to_uppercase() == "COUNT" {
                    Some(parse.next_int()?)
                } else {
                    return Err("Invalid count format".into());
                }
            }
            Err(parser::Error::EndOfStream) => None,
            Err(err) => return Err(err.into()),
        };
        Ok(XRange { command_size: parse.command_size(), key, start, end, count })
    }
}

#[async_trait]
impl Applicable for XRange {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let resp = match dst.db().xrange(self.key, self.start, self.end, self.count).await {
            Ok(entries) => {
                let mut arr = Vec::new();
                for entry in entries {
                    arr.push(entry.encode());
                }
                Type::Array(arr)
            }
            Err(e) => Type::SimpleError(e.to_string()),
        };
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}