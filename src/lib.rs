pub mod resp;
pub mod encoder;
pub mod parser;
pub mod cmd;
pub mod connection;
pub mod listener;
pub mod db;
pub mod replication;
pub mod engine;
pub mod rdb;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;