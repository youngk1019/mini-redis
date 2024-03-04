use std::time::SystemTime;
use bytes::Bytes;

#[derive(Debug, PartialEq, Clone)]
pub struct Order {
    pub dataset: u32,
    pub rtype: Type,
    pub expire: Option<SystemTime>,
}


#[derive(Debug, PartialEq, Clone)]
pub enum Type {
    String(String, Bytes), // key, value
    List(String, Vec<Bytes>), // key, values
    Set(String, Vec<Bytes>), // key, values
    SortedSet(String, Vec<(Bytes, f64)>), // key, values(value, score
    Hash(String, Vec<(Bytes, Bytes)>), // key, values(field, value)
}