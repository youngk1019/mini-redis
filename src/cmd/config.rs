use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, PartialEq)]
pub struct Config {
    command_size: u64,
    ask: Option<String>,
}

impl TryFrom<&mut Parse> for Config {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        match parse.next_string()?.to_uppercase().as_str() {
            "GET" => Ok(Config { command_size: parse.command_size(), ask: Some(parse.next_string()?) }),
            _ => Err("CONFIG subcommand not supported".into()),
        }
    }
}

#[async_trait]
impl Applicable for Config {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if dst.need_update_offset().await {
            dst.db().role().await.add_offset(self.command_size);
        }
        let mut resp = vec![Type::BulkString("unsupported CONFIG subcommand".into())];
        if let Some(ask) = self.ask {
            match ask.to_lowercase().as_str() {
                "dir" => {
                    resp = vec![Type::BulkString("dir".into()),
                                Type::BulkString(dst.db().dir().await.into())];
                }
                "dbfilename" => {
                    resp = vec![Type::BulkString("dbfilename".into()),
                                Type::BulkString(dst.db().file_name().await.into())];
                }
                _ => {}
            }
        }
        dst.write_all(Encoder::encode(&Type::Array(resp)).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}