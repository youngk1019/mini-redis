use std::ops::Add;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{BufReader};
use tokio::io::duplex;
use redis_starter_rust::rdb::parser::Parser;
use redis_starter_rust::rdb::serializer::Serializer;
use redis_starter_rust::rdb::types::{Type, Order};

#[tokio::test]
async fn test_string_parser() {
    let file = File::open(&Path::new("./tests/rdb/integer_keys.rdb")).await.unwrap();
    let reader = BufReader::new(file);
    let mut parser = Parser::new(reader);
    parser.parse().await.unwrap();
    assert_eq!(
        vec![
            Order { dataset: 0, rtype: Type::String("183358245".into(), "Positive 32 bit integer".into()), expire: None },
            Order { dataset: 0, rtype: Type::String("125".into(), "Positive 8 bit integer".into()), expire: None },
            Order { dataset: 0, rtype: Type::String("-29477".into(), "Negative 16 bit integer".into()), expire: None },
            Order { dataset: 0, rtype: Type::String("-123".into(), "Negative 8 bit integer".into()), expire: None },
            Order { dataset: 0, rtype: Type::String("43947".into(), "Positive 16 bit integer".into()), expire: None },
            Order { dataset: 0, rtype: Type::String("-183358245".into(), "Negative 32 bit integer".into()), expire: None },
        ],
        parser.orders().map(|v| v.clone()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_expiry_parser() {
    let file = File::open(&Path::new("./tests/rdb/keys_with_expiry.rdb")).await.unwrap();
    let reader = BufReader::new(file);
    let mut parser = Parser::new(reader);
    parser.parse().await.unwrap();
    assert_eq!(
        vec![
            Order { dataset: 0, rtype: Type::String("expires_ms_precision".into(), "2022-12-25 10:11:12.573 UTC".into()), expire: Some(UNIX_EPOCH.add(Duration::from_secs(1671963072)).add(Duration::from_millis(573))) },
        ],
        parser.orders().cloned().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_serializer() {
    let (mut tx, rx) = duplex(4 * 1024);
    let mut serializer = Serializer::new(&mut tx);
    serializer.init().await.unwrap();
    let orders = vec![
        Order { dataset: 0, rtype: Type::String("183358245".into(), "Positive 32 bit integer".into()), expire: None },
        Order { dataset: 1, rtype: Type::String("125".into(), "Positive 8 bit integer".into()), expire: None },
        Order { dataset: 0, rtype: Type::String("-29477".into(), "Negative 16 bit integer".into()), expire: Some(UNIX_EPOCH.add(Duration::from_secs(1671963072))) },
        Order { dataset: 0, rtype: Type::String("-123".into(), "Negative 8 bit integer".into()), expire: None },
        Order { dataset: 1, rtype: Type::String("43947".into(), "Positive 16 bit integer".into()), expire: None },
        Order { dataset: 0, rtype: Type::String("-183358245".into(), "Negative 32 bit integer".into()), expire: None },
    ];
    for order in orders.iter() {
        serializer.write_order(order).await.unwrap();
    }
    serializer.finish().await.unwrap();
    drop(tx);
    let mut parser = Parser::new(rx);
    parser.parse().await.unwrap();
    assert_eq!(
        orders,
        parser.orders().cloned().collect::<Vec<_>>()
    );
}