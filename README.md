# Redis Clone

A Redis clone implementation in C++ that supports basic Redis functionality including key-value operations, transactions, streams, and replication. Built 
with along with the code crafters build you're own redis challange: https://app.codecrafters.io/courses/redis/overview

## Features

- Key-Value Operations: SET, GET, INCR
- Transactions: MULTI, EXEC, DISCARD
- Streams: XADD, XRANGE, XREAD (with blocking support)
- Replication: Master-Replica architecture
- Persistence: RDB file loading and saving
- RESP Protocol: Full support for Redis serialization protocol

## Building
Prerequisites

- C++17 compatible compiler (clang++ recommended)
- POSIX-compliant operating system (Linux or macOS)

```bash
make
```

## Running the Server

```bash
./bin [options]
```

### Command-line Options

- `--port <port>`: TCP port to listen on (default: 6379)
- `--replicaof <host> <port>`: Make this server a replica of the specified master
- `--dir <directory>`: Directory for RDB files
- `--dbfilename <filename>`: Filename for RDB files
- `--help`: Show help

### Examples

Start a standalone server:
```bash
./bin --port 6379
```

Start a replica server:
```bash
./bin --port 6380 --replicaof localhost 6379
```

## Using the Client

A Python client is included in the `client` directory.

```bash
# Run in interactive mode
python client/redis_client.py --host localhost --port 6379
or 
python3 client/redis_client.py --host localhost --port 6379

# Run demo commands
python client/redis_client.py --host localhost --port 6379 --demo
or
python3 client/redis_client.py --host localhost --port 6379 --demo

```

### Interactive Mode

In interactive mode, you can type Redis commands directly:

```
redis> SET mykey "Hello World"
OK
redis> GET mykey
Hello World
redis> INCR counter
1
```

### Available Commands

The client supports the following commands:

- Basic: PING, ECHO, SET, GET, INCR, KEYS, TYPE
- Transactions: MULTI, EXEC, DISCARD
- Streams: XADD, XRANGE, XREAD
- Info and Configuration: INFO, CONFIG GET

## Project Structure

- `main.cpp`: Entry point and command-line parsing
- `server.cpp/hpp`: Core server implementation
- `tcp_socket.cpp/hpp`: TCP socket handling
- `resp.cpp/hpp`: RESP protocol implementation
- `streams.cpp/hpp`: Stream data type implementation
- `replication.cpp/hpp`: Master-replica replication
- `rdb.cpp/hpp`: RDB file persistence
- `client/redis_client.py`: Python client implementation

## Implementation Details

### RESP Protocol

The RESP (Redis Serialization Protocol) is fully implemented, supporting:
- Simple strings ("+OK\r\n")
- Errors ("-ERR message\r\n")
- Integers (":1000\r\n")
- Bulk strings ("$5\r\nHello\r\n")
- Arrays ("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

### Replication

The replication system supports:
- Initial handshake and RDB transfer
- Command propagation from master to replicas
- WAIT command for synchronous replication
- Replica promotion

### Streams

The stream implementation includes:
- Entry creation with auto-generated or custom IDs
- Range queries
- Blocking reads with timeout support

## Limitations

Unlike the real Redis:
- Limited command set
- Simplified data structures
- Multi-threaded instead of event-loop architecture
- Basic persistence implementation
- Limited data types (primarily strings and streams)

## License

This project is open source and available under the [MIT License](LICENSE).
