mod config;
mod error;
mod network;
mod pb;
pub mod service;
mod storage;

use std::sync::Arc;

use anyhow::Result;
pub use config::*;
pub use error::KvError;

pub use network::*;
pub use pb::abi::*;
pub use service::*;
pub use storage::*;
use tokio::{
    net::{TcpListener, TcpStream},
    time,
};
use tokio_rustls::client;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{info, info_span, instrument};

// build  k-v server with config
#[instrument(skip_all)]
pub async fn start_server_with_config(config: &ServerConfig) -> Result<()> {
    let cert = config.tls.cert.clone();
    let key = config.tls.key.clone();
    let ca = config.tls.ca.clone();

    let acceptor = TlsServerAcceptor::new(&cert, &key, ca.as_deref())?;

    let addr = &config.general.addr;
    match &config.storage {
        StorageConfig::MemTable => start_tls_server(addr, MemTable::new(), acceptor).await?,
        StorageConfig::SledDb(path) => start_tls_server(addr, SledDb::new(path), acceptor).await?,
    };

    Ok(())
}

// build k-v client with config
#[instrument(skip_all)]
pub async fn start_client_with_config(
    config: &ClientConfig,
) -> Result<YamuxCtrl<client::TlsStream<TcpStream>>> {
    let addr = &config.general.addr;
    let tls = &config.tls;
    let identity = tls
        .identity
        .as_ref()
        .map(|(cert, key)| (cert.as_str(), key.as_str()));
    let connector = TlsClientConnector::new(&tls.domain, identity, tls.ca.as_deref())?;
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    Ok(YamuxCtrl::new_client(stream, None))
}

async fn start_tls_server<T: Storage>(
    addr: &str,
    store: T,
    acceptor: TlsServerAcceptor,
) -> Result<()> {
    let service: Service<T> = ServiceInner::new(store).into();
    let broadcaster = service.broadcaster.clone();
    tokio::spawn(start_gc(broadcaster));

    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let root = info_span!("server_process");
        let _enter = root.enter();
        let tls = acceptor.clone();
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);

        let svc = service.clone();
        tokio::spawn(async move {
            let stream = tls.accept(stream).await.unwrap();
            YamuxCtrl::new_server(stream, None, move |stream| {
                let svc1 = svc.clone();
                async move {
                    let stream = ProstServerStream::new(stream.compat(), svc1.clone());
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        });
    }
}

async fn start_gc(broadcaster: Arc<Broadcaster>) {
    let mut interval = time::interval(std::time::Duration::from_secs(60 * 60)); // 1 hour
    loop {
        interval.tick().await;
        info!("Running garbage collection");
        gc_subscriptions(broadcaster.clone()).await;
    }
}
