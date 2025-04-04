use futures::{future, Future, TryStreamExt};
use std::marker::PhantomData;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, ConnectionError, Control, Mode, WindowUpdateMode};

use super::ProstClientStream;

pub struct YamuxCtrl<S> {
    pub ctrl: Control,
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        Self::new(stream, config, false, f)
    }

    fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        let mut config = config.unwrap_or_default();
        config.set_window_update_mode(WindowUpdateMode::OnRead);

        let conn = Connection::new(stream.compat(), config, mode);

        let ctrl = conn.control();

        tokio::spawn(yamux::into_stream(conn).try_for_each_concurrent(None, f));

        Self {
            ctrl,
            _conn: PhantomData,
        }
    }

    // open a new stream
    pub async fn open_stream(
        &mut self,
    ) -> Result<ProstClientStream<Compat<yamux::Stream>>, ConnectionError> {
        let stream = self.ctrl.open_stream().await?;
        Ok(ProstClientStream::new(stream.compat()))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crate::{
        assert_res_ok,
        network::tls::utils::{tls_acceptor, tls_connector},
        CommandRequest, KvError, MemTable, ProstServerStream, Service, ServiceInner, Storage,
        TlsServerAcceptor,
    };

    use anyhow::Result;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::server;
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    use tracing::warn;

    use super::*;

    #[tokio::test]
    async fn yamux_ctrl_client_server_should_work() -> Result<()> {
        let acceptor = tls_acceptor(false)?;
        let addr = start_yamux_server("127.0.0.1:0", acceptor, MemTable::new()).await?;

        let connector = tls_connector(false)?;
        let stream = TcpStream::connect(addr).await?;
        let stream = connector.connect(stream).await?;

        // build yamux client with TLS
        let mut ctrl = YamuxCtrl::new_client(stream, None);

        // open a new stream in client ctrl
        let mut stream = ctrl.open_stream().await?;

        let cmd = CommandRequest::hset("t1", "hello", "world".into());
        stream.execute_unary(cmd).await.unwrap();

        let cmd = CommandRequest::hget("t1", "hello");
        let res = stream.execute_unary(cmd).await.unwrap();

        assert_res_ok(&res, &["world".into()], &[]);
        Ok(())
    }

    pub async fn start_server_with<T>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: T,
        f: impl Fn(server::TlsStream<TcpStream>, Service) + Send + Sync + 'static,
    ) -> Result<SocketAddr, KvError>
    where
        T: Storage,
        Service: From<ServiceInner<T>>,
    {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service: Service = ServiceInner::new(store).into();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => match tls.accept(stream).await {
                        Ok(stream) => f(stream, service.clone()),
                        Err(e) => warn!("Failed to process TLS:{:?}", e),
                    },
                    Err(e) => warn!("Failed to accept TCP:{:?}", e),
                }
            }
        });

        Ok(addr)
    }

    // build a yamux server
    pub async fn start_yamux_server<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let f = |stream, service: Service| {
            YamuxCtrl::new_server(stream, None, move |s| {
                let svc = service.clone();
                async move {
                    let stream = ProstServerStream::new(s.compat(), svc);
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        };

        start_server_with(addr, tls, store, f).await
    }
}
