use crate::resp::Type;

use bytes::Bytes;
use std::{fmt, str, vec};

#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Type>,
}

#[derive(Debug)]
pub(crate) enum Error {
    EndOfStream,
    Other(crate::Error),
}

impl Parse {
    pub(crate) fn new(val: Type) -> Result<Parse, Error> {
        let array = match val {
            Type::Array(array) => array,
            val => return Err(format!("protocol error; expected array, got {:?}", val).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// Return the next entry. Array frames are arrays of frames, so the next
    /// entry is a frame.
    fn next(&mut self) -> Result<Type, Error> {
        self.parts.next().ok_or(Error::EndOfStream)
    }

    pub(crate) fn next_string(&mut self) -> Result<String, Error> {
        match self.next()? {
            Type::SimpleString(s) => Ok(s),
            Type::BulkString(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            val => Err(format!("protocol error; expected simple frame or bulk frame, got {:?}", val).into()),
        }
    }

    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, Error> {
        match self.next()? {
            Type::SimpleString(s) => Ok(Bytes::from(s.into_bytes())),
            Type::BulkString(data) => Ok(data),
            val => Err(format!("protocol error; expected simple frame or bulk frame, got {:?}", val).into()),
        }
    }

    pub(crate) fn next_int(&mut self) -> Result<u64, Error> {
        use atoi::atoi;
        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            // An integer frame type is already stored as an integer.
            Type::Integer(v) => Ok(v),
            // Simple and bulk frames must be parsed as integers. If the parsing
            // fails, an error is returned.
            Type::SimpleString(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Type::BulkString(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            val => Err(format!("protocol error; expected int frame but got {:?}", val).into()),
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {}
