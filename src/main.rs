use std::io::{BufRead, Write};
use std::net::TcpListener;

struct Encoder;

impl Encoder {
    pub fn encode_string(s: impl Into<String>) -> String {
        format!("+{}\r\n", s.into())
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                loop {
                    let mut reader = std::io::BufReader::new(&stream);
                    let mut buf = vec![];
                    match reader.read_until(b'\n', &mut buf) {
                        Ok(0) => break,
                        Ok(_) => stream.write_all(Encoder::encode_string("PONG").as_bytes()).unwrap(),
                        Err(e) => panic!("{}", e),
                    }
                }
            }
            Err(e) => panic!("{}", e),
        }
    }
}