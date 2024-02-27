use crate::resp::Type;

pub struct Encoder;

impl Encoder {
    pub fn encode(frame: &Type) -> Vec<u8> {
        let mut buf = Vec::new();
        match frame {
            Type::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Type::SimpleError(msg) => {
                buf.push(b'-');
                buf.extend_from_slice(msg.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Type::Integer(i) => {
                buf.push(b':');
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Type::BulkString(b) => {
                buf.push(b'$');
                buf.extend_from_slice(b.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(b.as_ref());
                buf.extend_from_slice(b"\r\n");
            }
            Type::Array(a) => {
                buf.push(b'*');
                buf.extend_from_slice(a.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for r in a {
                    buf.extend_from_slice(&Encoder::encode(r));
                }
            }
            Type::Boolean(b) => {
                buf.push(b'#');
                if *b { buf.push(b't') } else { buf.push(b'f') }
                buf.extend_from_slice(b"\r\n");
            }
            Type::Null => {
                buf.extend_from_slice(b"$-1\r\n");
            }
        }
        buf
    }
}