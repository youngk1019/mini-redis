use std::time::SystemTime;
use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use super::consts::{constant, op_code, encoding_type, length};
use super::types::{Type, Order};
use super::writer::Crc64AsyncWriter;

pub(crate) async fn write_length<W: AsyncWrite + Unpin>(output: &mut W, length: u32) -> crate::Result<()> {
    match length {
        0..=63 => {
            output.write_u8((length::RDB_6BITLEN << 6) | (length as u8)).await?;
        }
        64..=16383 => {
            output.write_u8((length::RDB_14BITLEN << 6) | ((length >> 8) & 0x3F) as u8).await?;
            output.write_u8((length & 0xFF) as u8).await?;
        }
        _ => {
            output.write_u8(length::RDB_32BITLEN << 6).await?;
            output.write_u32(length).await?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn write_encoding<W: AsyncWrite + Unpin>(output: &mut W, encoding: u32) -> crate::Result<()> {
    output.write_u8((length::RDB_ENCVAL << 6) | (encoding as u8)).await?;
    Ok(())
}

pub(crate) async fn write_blob<W: AsyncWrite + Unpin>(output: &mut W, blob: &Bytes) -> crate::Result<()> {
    write_length(output, blob.len() as u32).await?;
    output.write_all(blob).await?;
    Ok(())
}

pub struct Serializer<W: AsyncWrite + Unpin> {
    output: Crc64AsyncWriter<W>,
    last_database: Option<u32>,
}

impl<W: AsyncWrite + Unpin> Serializer<W> {
    pub fn new(output: W) -> Self {
        Self {
            output: Crc64AsyncWriter::new(output),
            last_database: None,
        }
    }

    pub async fn init(&mut self) -> crate::Result<()> {
        self.output.write_all(constant::RDB_MAGIC.as_bytes()).await?;
        self.output.write_all(constant::RDB_VERSION.as_bytes()).await?;
        Ok(())
    }

    pub async fn write_aux(&mut self, key: &Bytes, val: &Bytes) -> crate::Result<()> {
        self.output.write_u8(op_code::AUX).await?;
        write_blob(&mut self.output, key).await?;
        write_blob(&mut self.output, val).await?;
        Ok(())
    }

    pub async fn write_resize_db(&mut self, db: u32, size: u32, expire: u32) -> crate::Result<()> {
        self.write_db(db).await?;
        write_length(&mut self.output, size).await?;
        write_length(&mut self.output, expire).await?;
        Ok(())
    }

    pub async fn write_order(&mut self, order: &Order) -> crate::Result<()> {
        self.write_db(order.dataset).await?;
        if let Some(expire) = &order.expire {
            self.write_expire(expire).await?;
        }
        match &order.rtype {
            Type::String(key, val) => {
                self.output.write_u8(encoding_type::STRING).await?;
                write_blob(&mut self.output, &key.clone().into()).await?;
                write_blob(&mut self.output, val).await?;
            }
            Type::List(key, val) => {
                self.output.write_u8(encoding_type::LIST).await?;
                write_blob(&mut self.output, &key.clone().into()).await?;
                write_length(&mut self.output, val.len() as u32).await?;
                for item in val {
                    write_blob(&mut self.output, item).await?;
                }
            }
            Type::Set(key, val) => {
                self.output.write_u8(encoding_type::SET).await?;
                write_blob(&mut self.output, &key.clone().into()).await?;
                write_length(&mut self.output, val.len() as u32).await?;
                for item in val {
                    write_blob(&mut self.output, item).await?;
                }
            }
            Type::SortedSet(key, val) => {
                self.output.write_u8(encoding_type::ZSET).await?;
                write_blob(&mut self.output, &key.clone().into()).await?;
                write_length(&mut self.output, val.len() as u32).await?;
                for (item, score) in val {
                    write_blob(&mut self.output, item).await?;
                    self.write_f64(*score).await?;
                }
            }
            Type::Hash(key, val) => {
                self.output.write_u8(encoding_type::HASH).await?;
                write_blob(&mut self.output, &key.clone().into()).await?;
                write_length(&mut self.output, val.len() as u32).await?;
                for (field, value) in val {
                    write_blob(&mut self.output, field).await?;
                    write_blob(&mut self.output, value).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> crate::Result<()> {
        self.output.write_u8(op_code::EOF).await?;
        self.output.write_u64(self.output.crc64()).await?;
        self.output.flush().await?;
        Ok(())
    }

    async fn write_db(&mut self, db: u32) -> crate::Result<()> {
        match self.last_database {
            Some(last) if last == db => {}
            _ => {
                self.output.write_u8(op_code::SELECTDB).await?;
                write_length(&mut self.output, db).await?;
                self.last_database = Some(db);
            }
        };
        Ok(())
    }

    async fn write_expire(&mut self, expire: &SystemTime) -> crate::Result<()> {
        let expire = match expire.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(expire) => expire,
            Err(e) => return Err(e.into()),
        };
        if (expire.as_millis() % 1000) == 0 {
            self.output.write_u8(op_code::EXPIRETIME).await?;
            self.output.write_u32_le(expire.as_secs() as u32).await?;
        } else {
            self.output.write_u8(op_code::EXPIRETIME_MS).await?;
            self.output.write_u64_le(expire.as_millis() as u64).await?;
        }
        Ok(())
    }

    async fn write_f64(&mut self, value: f64) -> crate::Result<()> {
        if value.is_nan() {
            self.output.write_u8(253).await?;
        } else if value.is_infinite() {
            self.output.write_u8(if value.is_sign_positive() { 254 } else { 255 }).await?;
        } else {
            let str = value.to_string();
            self.output.write_u8(str.len() as u8).await?;
            self.output.write_all(str.as_bytes()).await?;
        }
        Ok(())
    }
}