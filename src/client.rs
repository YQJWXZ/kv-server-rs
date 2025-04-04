use anyhow::Result;
use kv_server_rs::{CommandRequest, ProstClientStream, TlsClientConnector};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let ca_sert = include_str!("../fixtures/ca.cert");
    let client_cert = include_str!("../fixtures/client.cert");
    let client_key = include_str!("../fixtures/client.key");
    let addr = "127.0.0.1:9527";
    let connector = TlsClientConnector::new(
        "kvserver.acme.inc",
        Some((client_cert, client_key)),
        Some(ca_sert),
    )?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;
    let mut client = ProstClientStream::new(stream);

    let cmd = CommandRequest::hset("t1", "hello", "world".to_string().into());

    let data = client.execute_unary(cmd).await?;

    info!("Got response: {:?}", data);
    Ok(())
}
