use tokio::net::TcpListener;
use clap::Parser;
use redis::{listener, db, replication};


#[derive(Parser)]
struct Config {
    #[clap(long, default_value_t = 6379)]
    port: usize,

    #[clap(long = "replicaof", number_of_values = 2)]
    replica: Option<Vec<String>>,

    #[clap(long, default_value = ".")]
    dir: String,

    #[clap(long, default_value = "dump.rdb")]
    dbfilename: String,
}


#[tokio::main]
async fn main() -> redis::Result<()> {
    let cfg = Config::parse();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.port)).await?;
    let mut role = None;
    if let Some(replica) = cfg.replica {
        role = Some(replication::role::Role::new_slave(cfg.port, replica[0].clone(), replica[1].parse().unwrap()));
    }
    let db = db::DB::new(cfg.dir, cfg.dbfilename, role).await;
    listener::Listener::new(db, listener).run().await
}