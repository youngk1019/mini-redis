use tokio::net::TcpListener;

use redis_starter_rust::{listener, db};


#[tokio::main]
async fn main() -> redis_starter_rust::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let db = db::DB::new();
    listener::Listener::new(db, listener).run().await
}