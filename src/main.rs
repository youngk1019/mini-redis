use tokio::net::TcpListener;
use clap::Parser;
use redis_starter_rust::{listener, db};


#[derive(Parser)]
struct Config {
    #[clap(long, default_value_t = 6379)]
    port: usize,
}


#[tokio::main]
async fn main() -> redis_starter_rust::Result<()> {
    let cfg = Config::parse();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port)).await?;
    let db = db::DB::new();
    listener::Listener::new(db, listener).run().await
}