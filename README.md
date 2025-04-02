# kv-server-rs

A high-performance key-value store server written in Rust with async support.

## Features

- In-memory and persistent (Sled) storage backends
- Async I/O using Tokio runtime
- TLS support for secure communication
- Yamux multiplexing for efficient connection handling
- Comprehensive logging with tracing

## Usage

### Certificate Generation

Generate certificates using the gen_cert example:

```bash
cargo run --example gen_cert
```

This will create:

- `fixtures/ca.cert`: CA certificate
- `fixtures/server.{cert,key}`: Server certificate and key
- `fixtures/client.{cert,key}`: Client certificate and key

To use custom domains/names, modify the example code:

```rust
let pem = creat_cert(&ca, &["your.domain.com"], "Your Server Name", false)?;
```

### Client Example (with TLS)

```rust
use kv_server_rs::{CommandRequest, ProstClientStream, TlsClientConnector};
use tokio::net::TcpStream;

// Setup TLS connector
let connector = TlsClientConnector::new(
    "kvserver.acme.inc",  // Server name
    Some((client_cert, client_key)),  // Client credentials
    Some(ca_cert)  // CA certificate
)?;

// Connect and execute command
let stream = TcpStream::connect("127.0.0.1:9527").await?;
let tls_stream = connector.connect(stream).await?;
let mut client = ProstClientStream::new(tls_stream);

let cmd = CommandRequest::hset("table1", "key", "value".into());
let response = client.execute(cmd).await?;
```

```bash
RUST_LOG=info cargo run --bin kv-c
```

### Server Example (with TLS)

```rust
use kv_server_rs::{MemTable, ProstServerStream, Service, ServiceInner, TlsServerAcceptor};
use tokio::net::TcpListener;

// Setup TLS
let acceptor = TlsServerAcceptor::new(
    server_cert,
    server_key,
    Some(ca_cert)
)?;

// Initialize service with storage backend
let service: Service = ServiceInner::new(MemTable::new()).into();

// Start server
let listener = TcpListener::bind("127.0.0.1:9527").await?;
loop {
    let (stream, _) = listener.accept().await?;
    let tls_stream = acceptor.accept(stream).await?;
    let server_stream = ProstServerStream::new(tls_stream, service.clone());
    tokio::spawn(async move { server_stream.process().await });
}
```

```bash
RUST_LOG=info cargo run --bin kv-s
```

### Core Components

- `ProstClientStream`/`ProstServerStream`: Protobuf serialization
- `TlsClientConnector`/`TlsServerAcceptor`: TLS management
- `CommandRequest`: Operations (hget/hset/hdel)
- `Service`: Request processing
- `Storage` traits: Memory/Sled implementations

## License

MIT
