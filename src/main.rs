use tokio::net::TcpListener;
use clap::Parser;
use redis_starter_rust::{listener, db};


#[derive(Parser)]
struct Config {
    #[clap(long, default_value_t = 6379)]
    port: usize,

    #[clap(long = "replicaof", number_of_values = 2)]
    replica: Option<Vec<String>>,
}


#[tokio::main]
async fn main() -> redis_starter_rust::Result<()> {
    let cfg = Config::parse();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port)).await?;
    let mut role: Option<db::Role> = None;
    if let Some(replica) = cfg.replica {
        role = Some(db::Role::new(format!("{}:{}", replica[0], replica[1])));
    }
    let db = db::DB::new(role);
    listener::Listener::new(db, listener).run().await
}