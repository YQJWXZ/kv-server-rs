use std::{io::Cursor, sync::Arc};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{
    client,
    rustls::{
        self, internal::pemfile, Certificate, ClientConfig, NoClientAuth, PrivateKey,
        RootCertStore, ServerConfig,
    },
    server,
    webpki::DNSNameRef,
    TlsAcceptor, TlsConnector,
};

use crate::KvError;

/// KV Server's ALPN(Application-Layer Protocol Negotiation)
const ALPN_KV: &str = "kv";

/// Store TLS ServerConfig and provide method accept to convert the underlying protocol into TLS
#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: Arc<ServerConfig>,
}

/// Store TLS Client and provide method connect to convert the underlying protocol into TLS
#[derive(Clone)]
pub struct TlsClientConnector {
    config: Arc<ClientConfig>,
    domain: Arc<String>,
}

impl TlsClientConnector {
    // load server cert/CA cert and generate ServerConfig
    pub fn new(
        domain: impl Into<String>,
        identity: Option<(&str, &str)>,
        server_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let mut config = ClientConfig::new();
        // if have identity, load the cert and key
        if let Some((cert, key)) = identity {
            let cert = load_certs(cert)?;
            let key = load_key(key)?;
            config.set_single_client_cert(cert, key)?;
        }

        // if have CA certificate for signing th server, load it so that the server certificate is not int the root certificate chain
        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert);
            config.root_store.add_pem_file(&mut buf).unwrap();
        } else {
            // load the root certificate chain of local trust
            let mut roots = RootCertStore::empty();
            for cert in
                rustls_native_certs::load_native_certs().expect("could not load platform certs")
            {
                roots.add(&rustls::Certificate(cert.to_vec())).unwrap();
            }
            config.root_store = roots;
        }

        Ok(Self {
            config: Arc::new(config),
            domain: Arc::new(domain.into()),
        })
    }

    /// Trigger the TLS protocol and convert the underlying stream into TLS stream
    pub async fn connect<S>(&self, stream: S) -> Result<client::TlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let dns = DNSNameRef::try_from_ascii_str(self.domain.as_str())
            .map_err(|_| KvError::Internal("Invalid DNS name".into()))?;

        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await?;

        Ok(stream)
    }
}

impl TlsServerAcceptor {
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;

        let mut config = match client_ca {
            Some(cert) => {
                // if the client certificate is issued by a CA certificate, load the CA certificate into the trust chain
                let mut cert = Cursor::new(cert);
                let mut client_root_cert_store = rustls::RootCertStore::empty();
                client_root_cert_store
                    .add_pem_file(&mut cert)
                    .map_err(|_| KvError::CertificateParseError("CA", "cert"))?;

                let client_auth = rustls::AllowAnyAuthenticatedClient::new(client_root_cert_store);

                ServerConfig::new(client_auth)
            }

            None => ServerConfig::new(NoClientAuth::new()),
        };

        // set the server certificate
        config
            .set_single_cert(certs, key)
            .map_err(|_| KvError::CertificateParseError("server", "cert"))?;

        config.set_protocols(&[Vec::from(ALPN_KV)]);

        Ok(Self {
            inner: Arc::new(config),
        })
    }

    /// Trigger the TLS protocol and convert the underlying stream into TLS stream
    pub async fn accept<S>(&self, stream: S) -> Result<server::TlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let acceptor = TlsAcceptor::from(self.inner.clone());
        Ok(acceptor.accept(stream).await?)
    }
}
fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let mut cert = Cursor::new(cert);
    pemfile::certs(&mut cert).map_err(|_| KvError::CertificateParseError("server", "cert"))
}

fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    let mut cursor = Cursor::new(key);

    if let Ok(mut keys) = pemfile::pkcs8_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    if let Ok(mut keys) = pemfile::rsa_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    Err(KvError::CertificateParseError("server", "key"))
}

#[cfg(test)]
pub mod utils {

    use crate::{KvError, TlsClientConnector};
    use anyhow::Result;

    use super::TlsServerAcceptor;

    const CA_CERT: &str = include_str!("../../fixtures/ca.cert");
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert");
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key");
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key");

    pub fn tls_connector(client_cert: bool) -> Result<TlsClientConnector, KvError> {
        let ca = Some(CA_CERT);
        let client_identity = Some((CLIENT_CERT, CLIENT_KEY));

        match client_cert {
            false => TlsClientConnector::new("kvserver.acme.inc", None, ca),
            true => TlsClientConnector::new("kvserver.acme.inc", client_identity, ca),
        }
    }

    pub fn tls_acceptor(client_cert: bool) -> Result<TlsServerAcceptor, KvError> {
        let ca = Some(CA_CERT);

        match client_cert {
            false => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, None),
            true => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::network::tls::utils::tls_connector;

    use super::utils::tls_acceptor;
    use anyhow::Result;
    use std::{net::SocketAddr, sync::Arc};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    #[tokio::test]
    async fn tls_should_work() -> Result<()> {
        let addr = start_server(false).await?;

        let connector = tls_connector(false)?;
        let stream = TcpStream::connect(addr).await?;

        // println!("[DEBUG] Connecting to server at {}", addr);
        let mut stream = connector.connect(stream).await
        // .map_err(|e| {
        //     println!("[DEBUG] TLS connection error: {:?}", e);
        //     e
        // })
        ?;

        // println!("[DEBUG] TLS handshake completed");

        stream.write_all(b"hello world!").await
        // .map_err(|e| {
        //     println!("[DEBUG] Write error: {:?}", e);
        //     e
        // })
        ?;

        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await
        // .map_err(|e| {
        //     println!("[DEBUG] Read error: {:?}", e);
        //     e
        // })
        ?;

        assert_eq!(&buf, b"hello world!");
        // println!("[DEBUG] Test completed successfully");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_client_cert_should_work() -> Result<()> {
        let addr = start_server(true).await.unwrap();

        let connector = tls_connector(true)?;

        let stream = TcpStream::connect(addr).await?;

        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_bad_domain_should_work() -> Result<()> {
        let addr = start_server(false).await?;
        let mut connector = tls_connector(false)?;
        connector.domain = Arc::new("kvserver1.acme.inc".into());
        let stream = TcpStream::connect(addr).await?;
        let res = connector.connect(stream).await;
        assert!(res.is_err());

        Ok(())
    }
    async fn start_server(client_cert: bool) -> Result<SocketAddr> {
        let accepter = tls_acceptor(client_cert)?;
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = accepter.accept(stream).await.unwrap();
            let mut buf = [0; 12];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        Ok(addr)
    }
}
