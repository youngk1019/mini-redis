use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use crate::connection::{Applicable, Connection};
use crate::encoder::Encoder;
use crate::parser;
use crate::parser::Parse;
use crate::resp::Type;

#[derive(Debug, Default, PartialEq)]
pub struct ReplConf {
    port: Option<usize>,
    ack: bool,
}

impl TryFrom<&mut Parse> for ReplConf {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let mut port = None;
        let mut ack = false;
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
                            ack = true;
                        }
                        _ => {}
                    }
                }
                Err(parser::Error::EndOfStream) => {
                    return Ok(
                        ReplConf {
                            port,
                            ack,
                        });
                }
                Err(e) => { return Err(e.into()); }
            }
        }
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
        } else if self.ack {
            let resp = Type::Array(vec![
                Type::BulkString("REPLCONF".into()),
                Type::BulkString("ACK".into()),
                Type::BulkString(dst.db().role().await.offset().to_string().into()),
            ]);
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