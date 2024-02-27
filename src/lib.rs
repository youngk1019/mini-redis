pub mod encoder;
pub mod resp;
pub mod cmd;
pub mod parser;
pub mod connection;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;