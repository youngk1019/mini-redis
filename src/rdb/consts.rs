pub(crate) mod version {
    pub const SUPPORTED_MINIMUM: u32 = 1;
    pub const SUPPORTED_MAXIMUM: u32 = 11;
}

pub(crate) mod constant {
    pub const RDB_MAGIC: &str = "REDIS";
    pub const RDB_VERSION: &str = "0007";
}

pub(crate) mod length {
    pub const RDB_6BITLEN: u8 = 0b00;
    pub const RDB_14BITLEN: u8 = 0b01;
    pub const RDB_32BITLEN: u8 = 0b10;
    pub const RDB_ENCVAL: u8 = 0b11;
}

pub(crate) mod op_code {
    pub const AUX: u8 = 250;
    pub const RESIZEDB: u8 = 251;
    pub const EXPIRETIME_MS: u8 = 252;
    pub const EXPIRETIME: u8 = 253;
    pub const SELECTDB: u8 = 254;
    pub const EOF: u8 = 255;
}

pub(crate) mod encoding_type {
    pub const STRING: u8 = 0;
    pub const LIST: u8 = 1;
    pub const SET: u8 = 2;
    pub const ZSET: u8 = 3;
    pub const HASH: u8 = 4;
    pub const HASH_ZIPMAP: u8 = 9;
    pub const LIST_ZIPLIST: u8 = 10;
    pub const SET_INTSET: u8 = 11;
    pub const ZSET_ZIPLIST: u8 = 12;
    pub const HASH_ZIPLIST: u8 = 13;
    pub const LIST_QUICKLIST: u8 = 14;
}

pub(crate) mod encoding {
    pub const INT8: u32 = 0;
    pub const INT16: u32 = 1;
    pub const INT32: u32 = 2;
    pub const LZF: u32 = 3;
}