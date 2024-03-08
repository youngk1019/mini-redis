# Mini-Redis

## Introduction

Mini-Redis is a simplistic yet powerful in-memory key-value (KV) database, meticulously developed using Rust to emulate the core functionalities of [Redis](https://redis.io). At its heart, Mini-Redis is designed to support the [RESP2](https://redis.io/docs/reference/protocol-spec/) (REdis Serialization Protocol), allowing for efficient data communication that mirrors Redis's own protocols.

The project embarks on providing a foundational platform that incorporates Redis's master-slave replication protocol, ensuring data redundancy and high availability. Additionally, Mini-Redis embraces the RDB (Redis Database) file format for data persistence, enabling secure and reliable data storage that can withstand restarts and system failures.

The project's storage and networking logic slightly differs from the actual Redis to better align with our primary goals of functionality and performance.

Mini-Redis draws inspiration from [tokio-mini-redis](https://github.com/tokio-rs/mini-redis) and [build-your-own-redis](https://github.com/codecrafters-io/build-your-own-redis).

## Running

This project primarily implements a master-slave server for in-memory key-value storage. 

The method to start the service is as follows:

```bash
cargo run --bin redis
```

Startup Parameters:
```bash
USAGE:
redis [OPTIONS]

OPTIONS:
--dbfilename <DBFILENAME>          RDB file name [default: dump.rdb]
--dir <DIR>                        RDB file directory [default: .]
-h, --help                             Print help information
--port <PORT>                      Port to listen on [default: 6379]
--replicaof <REPLICA> <REPLICA>    Replicate to master server [ip, port]
```

## KV Storage

Mini-Redis's KV storage is built on a hash map for storing key-value pairs and an ordered set for managing key expiration and eviction.

Mini-Redis supports the following commands:

- **[GET](https://redis.io/commands/get/)**: Retrieve the value associated with a given key. If the key does not exist, the command returns `nil`.

- **[SET](https://redis.io/commands/set/)**: Insert or update the value associated with a specific key. If the key already exists, its value is updated; if it does not, a new key-value pair is created. This command has been extended to support expiration eviction. This feature allows users to specify a time-to-live (TTL) for each key-value pair. Once the TTL expires, the key is automatically removed from the storage, making it an effective mechanism for managing data lifecycle and memory usage.

- **[DEL](https://redis.io/commands/del/)**: Delete a specific key-value pair from the database. the command returns the number of keys deleted.

- **[KEYS](https://redis.io/commands/keys/)**: List all the keys stored in the database. This command allows users to obtain a snapshot of all the keys currently managed by Mini-Redis, providing insights into the stored data. This command now supports pattern matching.

## Replication

Mini-Redis's replication feature is intricately designed around Redis's master-slave replication protocol, employing a sophisticated blend of [psync](https://redis.io/commands/psync/) and [replconf](https://redis.io/commands/replconf/) commands for seamless data transfer and synchronization. The replication process initiates with the `psync` command, enabling a slave to fetch a snapshot of the dataset from the master through an RDB file. This ensures a base level of consistency between the master and the slave. Post-initial sync, the `psync` command facilitates incremental data updates by transmitting newly executed commands from the master to the slave. Meanwhile, the `replconf` command configures replication settings and ensures robust communication pathways between the master and its slaves. To maintain synchronization accuracy, both master and slave track data offsets, determining the extent of data replication. Additionally, the [wait](https://redis.io/commands/wait/) command serves as a tool for querying the replication status, allowing for a consistency check on the data acknowledged by the slaves. This comprehensive approach, inspired by Redis's proven replication mechanisms, ensures Mini-Redis achieves high levels of data consistency and availability in distributed environments.

## Persistence

Mini-Redis incorporates data persistence through the use of the Redis Database (RDB) format, capturing the state of the in-memory database at specified intervals or triggers. This functionality ensures that data is not lost even after the server restarts, providing a robust mechanism for data recovery. The implementation of the RDB format in Mini-Redis closely mirrors that of Redis, supporting a wide range of file formats for serialization. However, one notable deviation from Redis's approach is the exclusion of support for zip-type reading due to the inability to employ copy-on-write during fork operations. Instead of leveraging a fork, which allows Redis to continue serving requests while persisting data, Mini-Redis requires a temporary pause in service to generate the persistence file. This limitation, while divergent from Redis's non-blocking persistence model, opens up avenues for optimization. (By adopting persistent data structures, Mini-Redis can potentially minimize the downtime required for creating persistence snapshots, thus mitigating the impact on service availability.)