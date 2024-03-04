use tokio::net::TcpListener;
use crate::db::DB;
use crate::connection::Connection;
use crate::replication;

pub struct Listener {
    db: DB,
    listener: TcpListener,
}

impl Listener {
    pub fn new(db: DB, listener: TcpListener) -> Self {
        Listener {
            db,
            listener,
        }
    }
    pub async fn run(&self) -> crate::Result<()> {
        let role = self.db.role().await;
        if !role.is_master() {
            let db = self.db.clone();
            tokio::spawn(async move {
                let _ = replication::build_replica_connect(db).await;
            });
        }
        loop {
            let (socket, _) = self.listener.accept().await?;
            let db = self.db.clone();
            let writeable = db.role().await.is_master();
            tokio::spawn(async move {
                let _ = Connection::new(socket, db, writeable).run().await;
            });
        }
    }
}