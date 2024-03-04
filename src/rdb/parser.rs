use std::collections::HashMap;
use std::ops::Add;

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt};

use lzf;
use super::consts::{version, constant, op_code, encoding_type, encoding, length};
use super::utils;
use super::types;

pub(crate) async fn verify_magic<R: AsyncRead + Unpin>(input: &mut R) -> crate::Result<()> {
    let mut magic = [0; 5];
    input.read_exact(&mut magic).await?;
    match magic == constant::RDB_MAGIC.as_bytes() {
        true => Ok(()),
        false => Err("invalid magic".into()),
    }
}

pub(crate) async fn verify_version<R: AsyncRead + Unpin>(input: &mut R) -> crate::Result<()> {
    let mut version = [0; 4];
    input.read_exact(&mut version).await?;
    let version = (version[0] - '0' as u8) as u32 * 1000
        + (version[1] - '0' as u8) as u32 * 100
        + (version[2] - '0' as u8) as u32 * 10
        + (version[3] - '0' as u8) as u32;
    match (version::SUPPORTED_MINIMUM..=version::SUPPORTED_MAXIMUM).contains(&version) {
        true => Ok(()),
        false => Err("unsupported version".into()),
    }
}

pub(crate) async fn read_length_with_encoding<R: AsyncRead + Unpin>(
    input: &mut R,
) -> crate::Result<(u32, bool)> {
    let length;
    let mut is_encoded = false;
    let enc_type = input.read_u8().await?;
    match (enc_type & 0xC0) >> 6 {
        length::RDB_ENCVAL => {
            is_encoded = true;
            length = (enc_type & 0x3F) as u32;
        }
        length::RDB_6BITLEN => {
            length = (enc_type & 0x3F) as u32;
        }
        length::RDB_32BITLEN => {
            length = input.read_u32().await?;
        }
        length::RDB_14BITLEN => {
            let next_byte = input.read_u8().await?;
            length = (((enc_type & 0x3F) as u32) << 8) | next_byte as u32;
        }
        _ => unreachable!()
    }
    Ok((length, is_encoded))
}

pub(crate) async fn read_length<R: AsyncRead + Unpin>(input: &mut R) -> crate::Result<u32> {
    let (length, _) = read_length_with_encoding(input).await?;
    Ok(length)
}

pub(crate) async fn read_blob<R: AsyncRead + Unpin>(input: &mut R) -> crate::Result<Vec<u8>> {
    let (length, is_encoded) = read_length_with_encoding(input).await?;
    if is_encoded {
        let result = match length {
            encoding::INT8 => utils::int_to_vec(input.read_i8().await? as i32),
            encoding::INT16 => utils::int_to_vec(input.read_i16_le().await? as i32),
            encoding::INT32 => utils::int_to_vec(input.read_i32_le().await?),
            encoding::LZF => {
                let compressed_length = read_length(input).await?;
                let real_length = read_length(input).await?;
                let data = utils::read_exact(input, compressed_length as usize).await?;
                match lzf::decompress(&data, real_length as usize) {
                    Ok(v) => v,
                    Err(e) => { return Err(format!("{}", e).into()); }
                }
            }
            _ => { return Err("invalid encoding".into()); }
        };
        Ok(result)
    } else {
        utils::read_exact(input, length as usize).await
    }
}

pub struct Parser<R: AsyncRead + Unpin> {
    input: R,
    meta_date: HashMap<String, String>,
    last_expired: Option<SystemTime>,
    last_database: u32,
    orders: Vec<types::Order>,
}

impl<R: AsyncRead + Unpin> Parser<R> {
    pub fn new(input: R) -> Parser<R> {
        Parser {
            input,
            meta_date: HashMap::new(),
            last_expired: None,
            last_database: 0,
            orders: vec![],
        }
    }
    pub fn orders(&self) -> impl Iterator<Item=&types::Order> {
        return self.orders.iter();
    }
    pub async fn parse(&mut self) -> crate::Result<()> {
        verify_magic(&mut self.input).await?;
        verify_version(&mut self.input).await?;
        self.last_database = 0;
        loop {
            let next_op = self.input.read_u8().await?;
            match next_op {
                op_code::SELECTDB => {
                    self.last_database = read_length(&mut self.input).await.unwrap();
                }
                op_code::EXPIRETIME_MS => {
                    let expire_time_ms = self.input.read_u64_le().await?;
                    self.last_expired = Some(UNIX_EPOCH.add(Duration::from_millis(expire_time_ms)))
                }
                op_code::EXPIRETIME => {
                    let expire_time = self.input.read_u32_le().await?;
                    self.last_expired = Some(UNIX_EPOCH.add(Duration::from_secs(expire_time as u64)))
                }
                op_code::AUX => {
                    let aux_key = read_blob(&mut self.input).await?;
                    let aux_val = read_blob(&mut self.input).await?;
                    self.meta_date.insert(String::from_utf8(aux_key)?, String::from_utf8(aux_val)?);
                }
                op_code::RESIZEDB => {
                    let db_size = read_length(&mut self.input).await?;
                    let expire_size = read_length(&mut self.input).await?;
                    self.meta_date.insert(format!("{}-db-size", self.last_database), db_size.to_string());
                    self.meta_date.insert(format!("{}-expire-size", self.last_database), expire_size.to_string());
                }
                op_code::EOF => {
                    let mut _checksum = Vec::new();
                    self.input.read_to_end(&mut _checksum).await?;
                    // TODO: verify checksum
                    break;
                }
                _ => {
                    let key = read_blob(&mut self.input).await?;
                    let key = String::from_utf8(key)?;
                    self.read_type(key, next_op).await?;
                    self.last_expired = None;
                }
            }
        }
        Ok(())
    }

    async fn read_type(&mut self, key: String, value_type: u8) -> crate::Result<()> {
        match value_type {
            encoding_type::STRING => self.read_string(key).await,
            encoding_type::LIST => self.read_list(key).await,
            encoding_type::SET => self.read_set(key).await,
            encoding_type::ZSET => self.read_sorted_set(key).await,
            encoding_type::HASH => self.read_hash(key).await,
            encoding_type::HASH_ZIPMAP => unimplemented!(),
            encoding_type::LIST_ZIPLIST => unimplemented!(),
            encoding_type::SET_INTSET => unimplemented!(),
            encoding_type::ZSET_ZIPLIST => unimplemented!(),
            encoding_type::HASH_ZIPLIST => unimplemented!(),
            encoding_type::LIST_QUICKLIST => unimplemented!(),
            _ => Err("invalid encoding".into()),
        }
    }

    async fn read_string(&mut self, key: String) -> crate::Result<()> {
        let val = read_blob(&mut self.input).await?;
        self.orders.push(types::Order {
            dataset: self.last_database,
            rtype: types::Type::String(key, val.into()),
            expire: self.last_expired,
        });
        Ok(())
    }

    async fn read_list(&mut self, key: String) -> crate::Result<()> {
        let len = read_length(&mut self.input).await?;
        let mut list = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let blob = read_blob(&mut self.input).await?;
            list.push(blob.into());
        }
        self.orders.push(types::Order {
            dataset: self.last_database,
            rtype: types::Type::List(key, list),
            expire: self.last_expired,
        });
        Ok(())
    }


    async fn read_set(&mut self, key: String) -> crate::Result<()> {
        let len = read_length(&mut self.input).await?;
        let mut list = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let blob = read_blob(&mut self.input).await?;
            list.push(blob.into());
        }
        self.orders.push(types::Order {
            dataset: self.last_database,
            rtype: types::Type::Set(key, list),
            expire: self.last_expired,
        });
        Ok(())
    }

    async fn read_sorted_set(&mut self, key: String) -> crate::Result<()> {
        let set_items = read_length(&mut self.input).await?;
        let mut items = Vec::with_capacity(set_items as usize);
        for _ in 0..set_items {
            let val = read_blob(&mut self.input).await?;
            let score_length = self.input.read_u8().await?;
            let score = match score_length {
                253 => { f64::NAN }
                254 => { f64::INFINITY }
                255 => { f64::NEG_INFINITY }
                _ => {
                    let tmp = utils::read_exact(&mut self.input, score_length as usize).await?;
                    std::str::from_utf8(&tmp)?.parse::<f64>()?
                }
            };
            items.push((val.into(), score));
        }
        self.orders.push(types::Order {
            dataset: self.last_database,
            rtype: types::Type::SortedSet(key, items),
            expire: self.last_expired,
        });
        Ok(())
    }

    async fn read_hash(&mut self, key: String) -> crate::Result<()> {
        let hash_items = read_length(&mut self.input).await?;
        let mut items = Vec::with_capacity(hash_items as usize);
        for _ in 0..hash_items {
            let field = read_blob(&mut self.input).await?;
            let val = read_blob(&mut self.input).await?;
            items.push((field.into(), val.into()));
        }
        self.orders.push(types::Order {
            dataset: self.last_database,
            rtype: types::Type::Hash(key, items),
            expire: self.last_expired,
        });
        Ok(())
    }
}