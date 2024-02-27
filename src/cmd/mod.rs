mod ping;
mod echo;
mod set;
mod get;


use crate::resp::Type;

use std::convert::TryFrom;
use async_trait::async_trait;
use crate::connection::Applicable;
use crate::parser::Parse;

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping(ping::Ping),
    Echo(echo::Echo),
    Set(set::Set),
    Get(get::Get),
}


impl TryFrom<Type> for Command {
    type Error = crate::Error;
    fn try_from(value: Type) -> crate::Result<Self> {
        let mut parse = Parse::new(value)?;
        let command_name = parse.next_string()?.to_lowercase();
        let command = match &command_name[..] {
            "ping" => Command::Ping((&mut parse).try_into()?),
            "echo" => Command::Echo((&mut parse).try_into()?),
            "set" => Command::Set((&mut parse).try_into()?),
            "get" => Command::Get((&mut parse).try_into()?),
            _ => unimplemented!(),
        };
        parse.finish()?;
        Ok(command)
    }
}

#[async_trait]
impl Applicable for Command {
    async fn apply(self, dst: &mut crate::connection::Connection) -> crate::Result<()> {
        match self {
            Command::Ping(ping) => ping.apply(dst).await,
            Command::Echo(echo) => echo.apply(dst).await,
            Command::Set(set) => set.apply(dst).await,
            Command::Get(get) => get.apply(dst).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use super::*;

    #[test]
    fn parse_ping() {
        let input = Type::Array(vec![Type::BulkString(Bytes::from("PING"))]);
        let expected = Command::Ping(ping::Ping::default());
        assert_eq!(Command::try_from(input).unwrap(), expected);
    }

    #[test]
    fn parse_echo() {
        let input = Type::Array(vec![
            Type::BulkString(Bytes::from("ECHO")),
            Type::BulkString(Bytes::from("Hello, world!")),
        ]);
        let expected = Command::Echo(echo::Echo::new(Bytes::from("Hello, world!")));
        assert_eq!(Command::try_from(input).unwrap(), expected);
    }

    #[test]
    fn parse_invalid_command() {
        let input = Type::Array(vec![]);
        assert_eq!(Command::try_from(input).is_err(), true);
    }
}