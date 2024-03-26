use bytes::Bytes;
use crate::resp;

#[derive(Debug, Clone)]
pub struct String {
    val: Bytes,
}

impl String {
    pub fn new(val: Bytes) -> Self {
        String { val }
    }
}

impl From<String> for Bytes {
    fn from(s: String) -> Bytes {
        s.val
    }
}

impl String {
    pub fn encode(&self) -> resp::Type {
        resp::Type::BulkString(self.val.clone())
    }
}
