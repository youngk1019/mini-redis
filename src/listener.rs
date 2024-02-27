use tokio::net::TcpListener;
use crate::db::DB;
use crate::connection::Connection;

pub struct Listener {
    db: DB,
    listener: TcpListener,
}

impl Listener {
    pub fn new(db: DB, listener: TcpListener) -> Self {
        Listener { db, listener }
    }
    pub async fn run(self) -> crate::Result<()> {
        loop {
            let (socket, _) = self.listener.accept().await?;
            let db = self.db.clone();
            tokio::spawn(async move {
                let _ = Connection::new(socket, db).run().await;
            });
        }
    }
}