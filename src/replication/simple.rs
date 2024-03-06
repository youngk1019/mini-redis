use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Simple {
    data: Bytes,
}

impl Simple {
    pub fn new(data: Bytes) -> Simple {
        Simple {
            data
        }
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }
}