use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, Default, PartialEq)]
pub struct ReplConf {
    command_size: u64,
    port: Option<usize>,
    ask_ack: bool,
    offset: u64,
}

impl TryFrom<&mut Parse> for ReplConf {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let mut port = None;
        let mut ask_ack = false;
        let mut offset = 0;
        loop {
            match parse.next_string() {
                Ok(msg) => {
                    match msg.to_uppercase().as_str() {
                        "LISTENING-PORT" => {
                            let listen_port = parse.next_int()?;
                            port = Some(listen_port as usize);
                        }
                        "GETACK" => {
                            let _ack = parse.next_string()?;
                            ask_ack = true;
                        }
                        "ACK" => {
                            offset = parse.next_int()?;
                        }
                        _ => {}
                    }
                }
                Err(parser::Error::EndOfStream) => {
                    return Ok(
                        ReplConf {
                            command_size: parse.command_size(),
                            port,
                            ask_ack,
                            offset,
                        });
                }
                Err(e) => { return Err(e.into()); }
            }
        }
    }
}

impl ReplConf {
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

#[async_trait]
impl Applicable for ReplConf {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        if let Some(port) = self.port {
            dst.set_port(port);
            let resp = Type::SimpleString("OK".to_string());
            dst.write_all(Encoder::encode(&resp).as_slice()).await?;
            dst.flush().await?;
        } else if self.ask_ack {
            let resp = Type::Array(vec![
                Type::BulkString("REPLCONF".into()),
                Type::BulkString("ACK".into()),
                Type::BulkString(dst.db().role().await.offset().to_string().into()),
            ]);
            dst.db().role().await.add_offset(self.command_size);
            dst.write_all(Encoder::encode(&resp).as_slice()).await?;
            dst.flush().await?;
        } else {
            // capabilities
            let resp = Type::SimpleString("OK".to_string());
            dst.write_all(Encoder::encode(&resp).as_slice()).await?;
            dst.flush().await?;
        }
        Ok(())
    }
}