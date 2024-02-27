use tokio::net::{TcpListener, TcpStream};
use tokio::io;

use redis_starter_rust::connection;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = handler(stream).await;
        });
    }
}

async fn handler(stream: TcpStream) -> redis_starter_rust::Result<()> {
    let mut con = connection::Connection::new(stream);
    con.run().await?;
    Ok(())
}