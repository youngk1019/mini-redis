use tokio::net::{TcpListener, TcpStream};
use crate::db::DB;
use crate::connection::Connection;
use crate::{connection, replication};

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
        if let Some((ip, port)) = self.db.role().await.master_info() {
            let master_address = format!("{}:{}", ip, port);
            let tcp_stream = TcpStream::connect(master_address).await?;
            let mut con = connection::Connection::new(tcp_stream, self.db.clone(), true);
            replication::replica_connect(&mut con).await?;
            tokio::spawn(async move {
                let _ = con.run().await;
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