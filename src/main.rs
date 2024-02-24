use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::io;

struct Encoder;

impl Encoder {
    pub fn encode_string(s: impl Into<String>) -> String {
        format!("+{}\r\n", s.into())
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = connection(stream).await;
        });
    }
}

async fn connection(stream: TcpStream) -> Result<(), io::Error> {
    let mut stream = BufReader::new(stream);
    let mut line = String::new();
    while stream.read_line(&mut line).await? > 0 {
        if line.to_ascii_uppercase().starts_with("PING") {
            stream.write_all(Encoder::encode_string("PONG").as_bytes()).await?;
        }
        line.clear();
    }
    Ok(())
}