use std::time::Duration;

use engineioxide_client::{EngineIoClientConfig, Sid, TransportType};
use socketioxide_core::{Str, parser::Parse};
use socketioxide_parser_common::CommonParser;
use tokio::time;

use crate::{
    driver::{self, Command, Link},
    errors::{ConnectError, DisconnectReason},
    socket::SocketBuilder,
    transport::WsFlavor,
};

/// A Socket.IO client: one Engine.IO websocket shared by the sockets of every namespace.
#[derive(Debug, Clone)]
pub struct Client<P = CommonParser> {
    link: Link<P>,
    sid: Sid,
    connect_timeout: Duration,
    ack_timeout: Duration,
}

impl Client {
    /// Starts configuring a client for the server at `url`: `ws://`, `http://`, or with the
    /// `rustls` feature `wss://` and `https://`.
    pub fn builder(url: impl Into<String>) -> ClientBuilder {
        ClientBuilder {
            url: url.into(),
            path: "/socket.io/".to_owned(),
            parser: CommonParser,
            connect_timeout: Duration::from_secs(20),
            ack_timeout: Duration::from_secs(5),
            #[cfg(feature = "rustls")]
            tls: None,
        }
    }
}

impl<P: Parse + Send + Sync + 'static> Client<P> {
    /// Returns the Engine.IO session id.
    pub fn sid(&self) -> Sid {
        self.sid
    }

    /// Prepares a socket on the namespace `ns`.
    pub fn socket(&self, ns: impl AsRef<str>) -> SocketBuilder<P> {
        let ns = ns.as_ref();
        let ns = if ns.starts_with('/') {
            Str::from(ns.to_owned())
        } else {
            Str::from(format!("/{ns}"))
        };
        SocketBuilder::new(
            self.link.clone(),
            ns,
            self.connect_timeout,
            self.ack_timeout,
        )
    }

    /// Closes the websocket; every socket ends with [`DisconnectReason::ClientClose`].
    pub fn close(&self) {
        self.link.send(Command::Close).ok();
    }

    /// Resolves once the websocket has ended, with the reason it ended.
    pub async fn closed(&self) -> DisconnectReason {
        self.link.closed().await
    }
}

/// Configures and connects a [`Client`].
#[derive(Debug, Clone)]
pub struct ClientBuilder<P = CommonParser> {
    url: String,
    path: String,
    parser: P,
    connect_timeout: Duration,
    ack_timeout: Duration,
    #[cfg(feature = "rustls")]
    tls: Option<std::sync::Arc<rustls::ClientConfig>>,
}

impl<P: Parse + Send + Sync + 'static> ClientBuilder<P> {
    /// Sets the Engine.IO path, `/socket.io/` by default.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Sets the parser, [`CommonParser`] by default.
    pub fn parser<P2: Parse + Send + Sync + 'static>(self, parser: P2) -> ClientBuilder<P2> {
        ClientBuilder {
            url: self.url,
            path: self.path,
            parser,
            connect_timeout: self.connect_timeout,
            ack_timeout: self.ack_timeout,
            #[cfg(feature = "rustls")]
            tls: self.tls,
        }
    }

    /// Sets how long the handshake and each namespace connection may take, 20 seconds by default.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the default acknowledgement timeout, 5 seconds by default.
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Sets the rustls configuration of `wss://` connections, in place of the root certificates
    /// of the `rustls-webpki-roots` and `rustls-native-roots` features.
    #[cfg(feature = "rustls")]
    pub fn tls_config(mut self, config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        self.tls = Some(config);
        self
    }

    /// Opens the websocket and completes the Engine.IO handshake.
    pub async fn connect(self) -> Result<Client<P>, ConnectError> {
        let (url, secure) = engine_url(&self.url, &self.path)?;
        let config = EngineIoClientConfig::builder()
            .uri(&url)
            .transports([TransportType::Websocket])
            .build()
            .map_err(|e| ConnectError::InvalidUrl(e.to_string()))?;
        #[cfg(feature = "rustls")]
        let flavor = match (secure, self.tls) {
            (false, _) => WsFlavor::default(),
            (true, Some(config)) => WsFlavor::with_tls(config),
            (true, None) => WsFlavor::with_tls(crate::tls::default_config()?),
        };
        #[cfg(not(feature = "rustls"))]
        let flavor = {
            debug_assert!(!secure);
            WsFlavor::default()
        };
        let connect = engineioxide_client::Client::connect(flavor, config);
        let engine = time::timeout(self.connect_timeout, connect)
            .await
            .map_err(|_| ConnectError::Timeout)??;
        Ok(Client {
            sid: engine.sid(),
            link: driver::spawn(engine, self.parser),
            connect_timeout: self.connect_timeout,
            ack_timeout: self.ack_timeout,
        })
    }
}

fn engine_url(url: &str, path: &str) -> Result<(String, bool), ConnectError> {
    let uri: http::Uri = url
        .parse()
        .map_err(|e: http::uri::InvalidUri| ConnectError::InvalidUrl(e.to_string()))?;
    let secure = match uri.scheme_str() {
        Some("ws" | "http") => false,
        #[cfg(feature = "rustls")]
        Some("wss" | "https") => true,
        Some(scheme) => return Err(ConnectError::UnsupportedScheme(scheme.to_owned())),
        None => return Err(ConnectError::InvalidUrl(format!("{url}: missing scheme"))),
    };
    let authority = uri
        .authority()
        .ok_or_else(|| ConnectError::InvalidUrl(format!("{url}: missing host")))?;
    let scheme = if secure { "wss" } else { "ws" };
    let prefix = uri.path().trim_end_matches('/');
    let path = path.trim_start_matches('/');
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    Ok((
        format!("{scheme}://{authority}{prefix}/{path}{query}"),
        secure,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(base: &str, path: &str) -> String {
        engine_url(base, path).unwrap().0
    }

    #[test]
    fn engine_urls() {
        assert_eq!(
            url("ws://localhost:3000", "/socket.io/"),
            "ws://localhost:3000/socket.io/"
        );
        assert_eq!(
            url("http://localhost:3000/", "/socket.io/"),
            "ws://localhost:3000/socket.io/"
        );
        assert_eq!(
            url("ws://host/api/?v=1", "custom/"),
            "ws://host/api/custom/?v=1"
        );
        assert!(matches!(
            engine_url("ftp://host", "/socket.io/"),
            Err(ConnectError::UnsupportedScheme(s)) if s == "ftp"
        ));
        assert!(matches!(
            engine_url("localhost:3000", "/socket.io/"),
            Err(ConnectError::InvalidUrl(_) | ConnectError::UnsupportedScheme(_))
        ));
    }

    #[test]
    #[cfg(feature = "rustls")]
    fn secure_engine_urls() {
        assert_eq!(
            engine_url("wss://host", "/socket.io/").unwrap(),
            ("wss://host/socket.io/".to_owned(), true)
        );
        assert_eq!(
            engine_url("https://host:8443/", "/socket.io/").unwrap(),
            ("wss://host:8443/socket.io/".to_owned(), true)
        );
    }

    #[test]
    #[cfg(not(feature = "rustls"))]
    fn secure_engine_urls_need_rustls() {
        assert!(matches!(
            engine_url("wss://host", "/socket.io/"),
            Err(ConnectError::UnsupportedScheme(s)) if s == "wss"
        ));
    }
}
