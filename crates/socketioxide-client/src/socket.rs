use std::{
    future::IntoFuture,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    time::Duration,
};

use futures_core::{Stream, future::BoxFuture};
use serde::{Serialize, de::DeserializeOwned};
use socketioxide_core::{
    Str, Value,
    packet::Packet,
    parser::{Parse, ParserError},
};
use socketioxide_parser_common::CommonParser;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use crate::{
    driver::{Command, Link, SocketState},
    errors::{AckError, ConnectError, DisconnectReason, SendError},
    packet,
};

/// Prepares the connection of a [`Socket`] to a namespace.
#[derive(Debug)]
pub struct SocketBuilder<P = CommonParser> {
    link: Link<P>,
    ns: Str,
    auth: Option<Result<Value, ParserError>>,
    timeout: Duration,
    ack_timeout: Duration,
}

impl<P: Parse + Send + Sync + 'static> SocketBuilder<P> {
    pub(crate) fn new(link: Link<P>, ns: Str, timeout: Duration, ack_timeout: Duration) -> Self {
        Self {
            link,
            ns,
            auth: None,
            timeout,
            ack_timeout,
        }
    }

    /// Sets the auth payload sent with CONNECT.
    pub fn auth<T: Serialize>(mut self, auth: T) -> Self {
        self.auth = Some(self.link.parser.encode_default(&auth));
        self
    }

    /// Sets how long to wait for the server to accept the namespace.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sends CONNECT and resolves once the server accepts or refuses the namespace.
    pub async fn connect(self) -> Result<(Socket<P>, Events<P>), ConnectError> {
        let auth = self.auth.transpose().map_err(SendError::Encode)?;
        let packet = self.link.encode(Packet::connect(self.ns.clone(), auth))?;
        let state = Arc::new(SocketState::new(self.ns));
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = Command::Connect {
            socket: state.clone(),
            packet,
            events: events_tx,
            reply: reply_tx,
        };
        if self.link.send(command).is_err() {
            return Err(ConnectError::Closed(self.link.closed_reason()));
        }
        let sid = match time::timeout(self.timeout, reply_rx).await {
            Ok(Ok(sid)) => sid?,
            Ok(Err(_)) => return Err(ConnectError::Closed(self.link.closed_reason())),
            Err(_) => {
                self.link.send(Command::Disconnect { socket: state }).ok();
                return Err(ConnectError::Timeout);
            }
        };
        let socket = Socket {
            link: self.link,
            state,
            sid,
            ack_timeout: self.ack_timeout,
        };
        Ok((socket, Events { rx: events_rx }))
    }
}

/// A socket connected to one namespace of the [`Client`](crate::Client) websocket.
#[derive(Debug, Clone)]
pub struct Socket<P = CommonParser> {
    link: Link<P>,
    state: Arc<SocketState>,
    sid: Arc<str>,
    ack_timeout: Duration,
}

impl<P: Parse + Send + Sync + 'static> Socket<P> {
    /// Returns the namespace of the socket.
    pub fn ns(&self) -> &str {
        &self.state.ns
    }

    /// Returns the id the server gave the socket on its namespace.
    pub fn id(&self) -> &str {
        &self.sid
    }

    /// Returns whether the socket is still connected to its namespace.
    pub fn connected(&self) -> bool {
        self.state.is_connected()
    }

    /// Sends an event without asking for an acknowledgement.
    pub fn emit<T: ?Sized + Serialize>(&self, event: &str, data: &T) -> Result<(), SendError> {
        self.ensure_connected()?;
        let value = self.link.parser.encode_value(data, Some(event))?;
        let packet = self
            .link
            .encode(packet::event(self.state.ns.clone(), value, None))?;
        self.link.send(Command::Send {
            socket: self.state.clone(),
            packet,
        })
    }

    /// Sends an event once awaited and resolves to the first argument of its acknowledgement.
    pub fn emit_with_ack<T: ?Sized + Serialize, V>(
        &self,
        event: &str,
        data: &T,
    ) -> AckRequest<V, P> {
        AckRequest {
            socket: self.clone(),
            value: self.link.parser.encode_value(data, Some(event)),
            timeout: self.ack_timeout,
            reply: PhantomData,
        }
    }

    /// Leaves the namespace; its event stream then ends with [`DisconnectReason::ClientDisconnect`].
    pub fn disconnect(&self) -> Result<(), SendError> {
        if !self.state.connected.swap(false, Ordering::AcqRel) {
            return Err(SendError::Disconnected);
        }
        self.link.send(Command::Disconnect {
            socket: self.state.clone(),
        })
    }

    fn ensure_connected(&self) -> Result<(), SendError> {
        if self.state.is_connected() {
            Ok(())
        } else {
            Err(SendError::Disconnected)
        }
    }
}

/// An event emitted with [`Socket::emit_with_ack`], sent when awaited.
#[must_use = "the event is only sent once the request is awaited"]
pub struct AckRequest<V, P = CommonParser> {
    socket: Socket<P>,
    value: Result<Value, ParserError>,
    timeout: Duration,
    reply: PhantomData<fn() -> V>,
}

impl<V, P> AckRequest<V, P> {
    /// Sets how long to wait for the acknowledgement.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl<V: DeserializeOwned, P: Parse + Send + Sync + 'static> AckRequest<V, P> {
    async fn send(self) -> Result<V, AckError> {
        let Self {
            socket,
            value,
            timeout,
            ..
        } = self;
        socket.ensure_connected()?;
        let value = value.map_err(SendError::Encode)?;
        let id = socket.state.next_ack.fetch_add(1, Ordering::Relaxed);
        let packet = socket
            .link
            .encode(packet::event(socket.state.ns.clone(), value, Some(id)))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        socket.link.send(Command::Ack {
            socket: socket.state.clone(),
            id,
            packet,
            reply: reply_tx,
        })?;
        let mut reply = match time::timeout(timeout, reply_rx).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => return Err(AckError::Disconnected),
            Err(_) => return Err(AckError::Timeout),
        };
        socket
            .link
            .parser
            .decode_value(&mut reply, false)
            .map_err(AckError::Decode)
    }
}

impl<V, P> IntoFuture for AckRequest<V, P>
where
    V: DeserializeOwned + Send + 'static,
    P: Parse + Send + Sync + 'static,
{
    type Output = Result<V, AckError>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.send())
    }
}

/// Answers a server event that waits for an acknowledgement.
#[derive(Debug)]
pub struct AckSender<P = CommonParser> {
    link: Link<P>,
    socket: Arc<SocketState>,
    id: i64,
}

impl<P: Parse> AckSender<P> {
    pub(crate) fn new(link: Link<P>, socket: Arc<SocketState>, id: i64) -> Self {
        Self { link, socket, id }
    }

    /// Sends the acknowledgement with `data` as its arguments.
    pub fn send<T: ?Sized + Serialize>(self, data: &T) -> Result<(), SendError> {
        if !self.socket.is_connected() {
            return Err(SendError::Disconnected);
        }
        let value = self.link.parser.encode_value(data, None)?;
        let packet = self
            .link
            .encode(Packet::ack(self.socket.ns.clone(), value, self.id))?;
        self.link.send(Command::Send {
            socket: self.socket,
            packet,
        })
    }
}

/// The arguments of a server event.
#[derive(Debug)]
pub struct Args<P = CommonParser> {
    value: Value,
    parser: P,
}

impl<P: Parse> Args<P> {
    pub(crate) fn new(value: Value, parser: P) -> Self {
        Self { value, parser }
    }

    /// Decodes the arguments: a tuple takes all of them, any other type takes the first one.
    pub fn decode<T: DeserializeOwned>(&mut self) -> Result<T, ParserError> {
        self.parser.decode_value(&mut self.value, true)
    }
}

/// An event the server sent to a [`Socket`].
#[derive(Debug)]
pub enum SocketEvent<P = CommonParser> {
    /// A server event.
    Event {
        /// The event name.
        name: String,
        /// The event arguments.
        args: Args<P>,
        /// Set when the server waits for an acknowledgement.
        ack: Option<AckSender<P>>,
    },
    /// The socket ended; nothing follows.
    Disconnected(DisconnectReason),
}

/// The events of a [`Socket`], ending with exactly one [`SocketEvent::Disconnected`].
#[derive(Debug)]
pub struct Events<P = CommonParser> {
    rx: mpsc::UnboundedReceiver<SocketEvent<P>>,
}

impl<P> Events<P> {
    /// Waits for the next event; `None` once the socket has ended.
    pub async fn recv(&mut self) -> Option<SocketEvent<P>> {
        self.rx.recv().await
    }
}

impl<P> Stream for Events<P> {
    type Item = SocketEvent<P>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
