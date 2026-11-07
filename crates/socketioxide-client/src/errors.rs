use std::{error::Error, fmt};

use socketioxide_core::parser::ParserError;

/// Why a socket, or the whole client, ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// The socket left its namespace through [`Socket::disconnect`](crate::Socket::disconnect).
    ClientDisconnect,
    /// The server sent DISCONNECT for the namespace.
    ServerDisconnect,
    /// The client was closed with [`Client::close`](crate::Client::close) or by dropping every handle.
    ClientClose,
    /// The server sent an Engine.IO close packet.
    ServerClose,
    /// The websocket received a close frame or reached the end of its stream.
    TransportClose,
    /// The websocket failed with an IO or protocol error.
    TransportError(String),
    /// No ping arrived within `ping_interval + ping_timeout` of the previous one.
    PingTimeout,
    /// A packet from the server could not be decoded.
    ParseError(String),
    /// The server sent a binary packet, which this client does not support.
    BinaryPacket,
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientDisconnect => f.write_str("client namespace disconnect"),
            Self::ServerDisconnect => f.write_str("server namespace disconnect"),
            Self::ClientClose => f.write_str("client closed"),
            Self::ServerClose => f.write_str("server closed"),
            Self::TransportClose => f.write_str("transport closed"),
            Self::TransportError(e) => write!(f, "transport error: {e}"),
            Self::PingTimeout => f.write_str("ping timeout"),
            Self::ParseError(e) => write!(f, "packet parsing error: {e}"),
            Self::BinaryPacket => f.write_str("binary packets are not supported"),
        }
    }
}

/// Errors of [`ClientBuilder::connect`](crate::ClientBuilder::connect) and
/// [`SocketBuilder::connect`](crate::SocketBuilder::connect).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    /// The url could not be parsed.
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    /// The url scheme is not `ws` or `http`, nor `wss` or `https` with the `rustls` feature.
    #[error("unsupported url scheme: {0}")]
    UnsupportedScheme(String),
    /// The TLS configuration could not be built.
    #[cfg(feature = "rustls")]
    #[error("tls configuration: {0}")]
    Tls(String),
    /// The Engine.IO handshake failed.
    #[error("engine.io connection failed")]
    Engine(#[from] engineioxide_client::errors::ConnectError),
    /// The server did not answer in time.
    #[error("connection timed out")]
    Timeout,
    /// The server refused the namespace with this CONNECT_ERROR message.
    #[error("namespace connection refused: {0}")]
    Refused(String),
    /// The CONNECT reply carries no session id.
    #[error("invalid CONNECT reply")]
    InvalidReply,
    /// The client already has a socket on this namespace.
    #[error("namespace {0} is already connected")]
    AlreadyConnected(String),
    /// The CONNECT packet could not be sent.
    #[error(transparent)]
    Send(#[from] SendError),
    /// The client ended before the namespace was connected.
    #[error("client closed: {0}")]
    Closed(DisconnectReason),
}

/// Errors when sending a packet.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SendError {
    /// The data could not be encoded.
    #[error("cannot encode packet: {0}")]
    Encode(#[from] ParserError),
    /// The encoded packet exceeds the `maxPayload` the server announced.
    #[error("packet of {size} bytes exceeds the server max payload of {max} bytes")]
    PayloadTooLarge {
        /// Size of the encoded Engine.IO packet.
        size: usize,
        /// The server `maxPayload`.
        max: u64,
    },
    /// The data contains binary attachments, which this client does not support.
    #[error("binary attachments are not supported")]
    Binary,
    /// The socket is no longer connected.
    #[error("socket is disconnected")]
    Disconnected,
}

/// Errors when waiting for an acknowledgement.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AckError {
    /// The event could not be sent.
    #[error(transparent)]
    Send(#[from] SendError),
    /// No acknowledgement arrived in time.
    #[error("ack timed out")]
    Timeout,
    /// The acknowledgement could not be decoded.
    #[error("cannot decode ack: {0}")]
    Decode(ParserError),
    /// The socket ended before the acknowledgement arrived.
    #[error("socket disconnected before the ack arrived")]
    Disconnected,
}

pub(crate) fn describe(err: &(dyn Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(err) = source {
        out.push_str(": ");
        out.push_str(&err.to_string());
        source = err.source();
    }
    out
}
