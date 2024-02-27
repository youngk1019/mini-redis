use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use bytes::{Buf, Bytes};

#[derive(Clone, fmt::Debug)]
pub enum Type {
    // RESP2
    SimpleString(String),
    SimpleError(String),
    Integer(u64),
    BulkString(Bytes),
    Array(Vec<Type>),
    // RESP3
    Null,
    Boolean(bool),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Type::SimpleString(s) => s.fmt(f),
            Type::SimpleError(s) => s.fmt(f),
            Type::Integer(i) => i.fmt(f),
            Type::BulkString(b) => String::from_utf8_lossy(b).fmt(f),
            Type::Array(a) => {
                write!(f, "[")?;
                for (i, r) in a.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    r.fmt(f)?;
                }
                write!(f, "]")
            }
            Type::Null => "null".fmt(f),
            Type::Boolean(b) => write!(f, "{}", if *b { "true" } else { "false" }),
        }
    }
}


impl Type {
    /// Checks if an entire message can be decoded from `cur`
    pub fn check(cur: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(cur)? {
            b'+' => {
                get_line(cur)?;
                Ok(())
            }
            b'-' => {
                get_line(cur)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(cur)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(cur)? {
                    // Skip '-1\r\n'
                    skip(cur, 4)
                } else {
                    // Read the bulk string
                    let len: usize = get_decimal(cur)?.try_into()?;
                    // skip that number of bytes + 2 (\r\n).
                    skip(cur, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(cur)?;
                for _ in 0..len {
                    Type::check(cur)?;
                }
                Ok(())
            }
            b'#' => {
                get_bool(cur)?;
                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    /// The message has already been validated with `check`.
    pub fn parse(cur: &mut Cursor<&[u8]>) -> Result<Type, Error> {
        match get_u8(cur)? {
            b'+' => {
                let line = get_line(cur)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Type::SimpleString(string))
            }
            b'-' => {
                let line = get_line(cur)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Type::SimpleError(string))
            }
            b':' => {
                let len = get_decimal(cur)?;
                Ok(Type::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(cur)? {
                    let line = get_line(cur)?;
                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }
                    Ok(Type::Null)
                } else {
                    let len = get_decimal(cur)?.try_into()?;
                    let n = len + 2;
                    if cur.remaining() < n {
                        return Err(Error::Incomplete);
                    }
                    let data = Bytes::copy_from_slice(&cur.chunk()[..len]);
                    // skip that number of bytes + 2 (\r\n).
                    skip(cur, n)?;
                    Ok(Type::BulkString(data))
                }
            }
            b'*' => {
                let len = get_decimal(cur)?.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Type::parse(cur)?);
                }
                Ok(Type::Array(out))
            }
            b'#' => {
                let b = get_bool(cur)?;
                Ok(Type::Boolean(b))
            }
            _ => unimplemented!(),
        }
    }
}

impl PartialEq<&str> for Type {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Type::SimpleString(s) => s.eq(other),
            Type::BulkString(s) => s.eq(other),
            _ => false,
        }
    }
}

fn peek_u8(cur: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !cur.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(cur.chunk()[0])
}

fn get_u8(cur: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !cur.has_remaining() {
        return Err(Error::Incomplete);
    }
    Ok(cur.get_u8())
}

fn skip(cur: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if cur.remaining() < n {
        return Err(Error::Incomplete);
    }
    cur.advance(n);
    Ok(())
}

/// Read a new-line terminated decimal
fn get_decimal(cur: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(cur)?;
    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

fn get_bool(cur: &mut Cursor<&[u8]>) -> Result<bool, Error> {
    let line = get_line(cur)?;
    if line.len() == 1 {
        match line[0] {
            b't' => Ok(true),
            b'f' => Ok(false),
            _ => Err("protocol error; invalid frame format".into()),
        }
    } else {
        Err("protocol error; invalid frame format".into())
    }
}

/// Find a line
fn get_line<'a>(cur: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = cur.position() as usize;
    let end = cur.get_ref().len() - 1;

    for i in start..end {
        if cur.get_ref()[i] == b'\r' && cur.get_ref()[i + 1] == b'\n' {
            cur.set_position((i + 2) as u64);
            return Ok(&cur.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
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

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}


impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
