use std::{
    future::IntoFuture,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use socketioxide::{SocketIo, SocketIoBuilder};
use socketioxide_client::{Client, DisconnectReason, Events, Socket, SocketEvent};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

pub const WAIT: Duration = Duration::from_secs(3);

pub async fn within<F: IntoFuture>(fut: F) -> F::Output {
    tokio::time::timeout(WAIT, fut)
        .await
        .expect("timed out waiting")
}

pub struct Server {
    pub io: SocketIo,
    pub url: String,
    pub connections: Arc<AtomicUsize>,
}

impl Server {
    pub async fn start(builder: SocketIoBuilder) -> Self {
        let (svc, io) = builder.build_svc();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                let conn = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc.clone())
                    .with_upgrades();
                tokio::spawn(conn);
            }
        });
        Self {
            io,
            url,
            connections,
        }
    }

    #[cfg(feature = "rustls")]
    pub async fn start_tls(builder: SocketIoBuilder) -> Self {
        let (svc, io) = builder.build_svc();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("wss://localhost:{}", listener.local_addr().unwrap().port());
        let acceptor = tls::acceptor();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                let (acceptor, svc) = (acceptor.clone(), svc.clone());
                tokio::spawn(async move {
                    let Ok(stream) = acceptor.accept(stream).await else {
                        return;
                    };
                    http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .with_upgrades()
                        .await
                        .ok();
                });
            }
        });
        Self {
            io,
            url,
            connections,
        }
    }

    pub async fn default() -> Self {
        Self::start(SocketIo::builder()).await
    }

    pub async fn client(&self) -> Client {
        within(Client::builder(&self.url).connect()).await.unwrap()
    }
}

pub async fn join(client: &Client, ns: &str) -> (Socket, Events) {
    within(client.socket(ns).connect()).await.unwrap()
}

pub async fn next_event(events: &mut Events) -> SocketEvent {
    within(events.recv()).await.expect("the event stream ended")
}

pub async fn disconnected(events: &mut Events) -> DisconnectReason {
    match next_event(events).await {
        SocketEvent::Disconnected(reason) => {
            assert!(
                within(events.recv()).await.is_none(),
                "an event followed Disconnected"
            );
            reason
        }
        SocketEvent::Event { name, .. } => panic!("expected Disconnected, got event {name}"),
    }
}

pub async fn quiet(events: &mut Events) {
    let next = tokio::time::timeout(Duration::from_millis(100), events.recv()).await;
    assert!(next.is_err(), "unexpected event: {:?}", next.unwrap());
}

pub struct FakeServer {
    pub url: String,
    listener: TcpListener,
}

impl FakeServer {
    pub async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        Self { url, listener }
    }

    pub async fn accept(
        &self,
        ping_interval: u64,
        ping_timeout: u64,
    ) -> WebSocketStream<TcpStream> {
        let (stream, _) = self.listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let open = format!(
            "0{{\"sid\":\"AAAAAAAAAAAAAAAA\",\"upgrades\":[],\"pingInterval\":{ping_interval},\"pingTimeout\":{ping_timeout},\"maxPayload\":100000}}"
        );
        ws.send(Message::text(open)).await.unwrap();
        ws
    }

    pub async fn client(
        &self,
        ping_interval: u64,
        ping_timeout: u64,
    ) -> (Client, WebSocketStream<TcpStream>) {
        let connect = Client::builder(&self.url).connect();
        let (client, ws) =
            within(async { tokio::join!(connect, self.accept(ping_interval, ping_timeout)) }).await;
        (client.unwrap(), ws)
    }
}

pub async fn fake_join(
    client: &Client,
    ws: &mut WebSocketStream<TcpStream>,
    ns: &str,
) -> (Socket, Events) {
    let accept = async {
        let Some(Ok(Message::Text(connect))) = ws.next().await else {
            panic!("expected a CONNECT packet");
        };
        assert!(connect.as_str().starts_with("40"), "{connect}");
        let reply = format!("{}{{\"sid\":\"fake-{ns}\"}}", connect.as_str());
        ws.send(Message::text(reply)).await.unwrap();
    };
    let (socket, ()) = within(async { tokio::join!(client.socket(ns).connect(), accept) }).await;
    socket.unwrap()
}

#[cfg(feature = "rustls")]
pub mod tls {
    use std::sync::Arc;

    use socketioxide_client::rustls::{
        ClientConfig, RootCertStore, ServerConfig,
        crypto::{CryptoProvider, ring},
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    };
    use tokio_rustls::TlsAcceptor;

    const CA: &[u8] = include_bytes!("certs/ca.pem");
    const CERT: &[u8] = include_bytes!("certs/server.pem");
    const KEY: &[u8] = include_bytes!("certs/server.key");

    fn provider() -> Arc<CryptoProvider> {
        Arc::new(ring::default_provider())
    }

    pub fn client_config(trust_test_ca: bool) -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        if trust_test_ca {
            roots
                .add(CertificateDer::from_pem_slice(CA).unwrap())
                .unwrap();
        }
        let config = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(config)
    }

    pub fn acceptor() -> TlsAcceptor {
        let cert = CertificateDer::from_pem_slice(CERT).unwrap();
        let key = PrivateKeyDer::from_pem_slice(KEY).unwrap();
        let config = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        TlsAcceptor::from(Arc::new(config))
    }
}
