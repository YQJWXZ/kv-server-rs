use std::sync::Arc;

use anyhow::Result;
use kv_server_rs::{MemTable, ProstServerStream, Service, ServiceInner, TlsServerAcceptor};
use tokio::{net::TcpListener, time};
use tracing::info;

use kv_server_rs::service::{subscribe_gc::gc_subscriptions, topic::Broadcaster};
async fn start_gc(broadcaster: Arc<Broadcaster>) {
    let mut interval = time::interval(std::time::Duration::from_secs(60 * 60)); // 1 hour
    loop {
        interval.tick().await;
        info!("Running garbage collection");
        gc_subscriptions(broadcaster.clone()).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let ca_cert = include_str!("../fixtures/ca.cert");
    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");

    let acceptor = TlsServerAcceptor::new(server_cert, server_key, Some(ca_cert))?;

    let service_inner = ServiceInner::new(MemTable::new());
    let broadcaster = service_inner.broadcaster.clone();
    let service: Service = service_inner.into();

    tokio::spawn(start_gc(broadcaster));

    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let tls = acceptor.clone();
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        let stream = tls.accept(stream).await?;
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move { stream.process().await });
    }
}
