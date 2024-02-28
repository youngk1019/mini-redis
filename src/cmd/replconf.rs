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
}

impl TryFrom<&mut Parse> for ReplConf {
    type Error = crate::Error;
    fn try_from(parse: &mut Parse) -> crate::Result<Self> {
        let mut port = None;
        loop {
            match parse.next_string() {
                Ok(msg) => {
                    match msg.to_uppercase().as_str() {
                        "LISTENING-PORT" => {
                            let listen_port = parse.next_int()?;
                            port = Some(listen_port as usize);
                        }
                        _ => {}
                    }
                }
                Err(parser::Error::EndOfStream) => { return Ok(ReplConf { port }); }
                Err(e) => { return Err(e.into()); }
            }
        }
    }
}

#[async_trait]
impl Applicable for ReplConf {
    async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let resp = Type::SimpleString("OK".to_string());
        dst.write_all(Encoder::encode(&resp).as_slice()).await?;
        dst.flush().await?;
        Ok(())
    }
}