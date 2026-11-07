use std::{
    collections::HashMap,
    mem,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use engineioxide_client::{
    EioEvent,
    errors::{ClientError, ClientErrorKind},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use socketioxide_core::{
    Str, Value,
    packet::{Packet, PacketData},
    parser::{Parse, ParseConfig, ParseError, ParserState},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

use crate::{
    errors::{ConnectError, DisconnectReason, SendError, describe},
    packet,
    socket::{AckSender, Args, SocketEvent},
    transport::WsFlavor,
};

pub(crate) type Engine = engineioxide_client::Client<WsFlavor>;

const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct SocketState {
    pub ns: Str,
    pub connected: AtomicBool,
    pub next_ack: AtomicI64,
}

impl SocketState {
    pub fn new(ns: Str) -> Self {
        Self {
            ns,
            connected: AtomicBool::new(false),
            next_ack: AtomicI64::new(0),
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}

pub(crate) enum Command<P> {
    Connect {
        socket: Arc<SocketState>,
        packet: EioEvent,
        events: mpsc::UnboundedSender<SocketEvent<P>>,
        reply: oneshot::Sender<Result<Arc<str>, ConnectError>>,
    },
    Send {
        socket: Arc<SocketState>,
        packet: EioEvent,
    },
    Ack {
        socket: Arc<SocketState>,
        id: i64,
        packet: EioEvent,
        reply: oneshot::Sender<Value>,
    },
    Disconnect {
        socket: Arc<SocketState>,
    },
    Close,
}

#[derive(Debug, Clone)]
pub(crate) struct Link<P> {
    tx: mpsc::UnboundedSender<Command<P>>,
    pub parser: P,
    max_payload: u64,
    closed: watch::Receiver<Option<DisconnectReason>>,
}

impl<P: Parse> Link<P> {
    pub fn encode(&self, packet: Packet) -> Result<EioEvent, SendError> {
        packet::encode(self.parser, self.max_payload, packet)
    }

    pub fn send(&self, command: Command<P>) -> Result<(), SendError> {
        self.tx.send(command).map_err(|_| SendError::Disconnected)
    }

    pub fn closed_reason(&self) -> DisconnectReason {
        self.closed
            .borrow()
            .clone()
            .unwrap_or(DisconnectReason::ClientClose)
    }

    pub async fn closed(&self) -> DisconnectReason {
        let mut closed = self.closed.clone();
        match closed.wait_for(Option::is_some).await {
            Ok(reason) => reason.clone().unwrap_or(DisconnectReason::ClientClose),
            Err(_) => DisconnectReason::ClientClose,
        }
    }

    fn downgrade(&self) -> WeakLink<P> {
        WeakLink {
            tx: self.tx.downgrade(),
            parser: self.parser,
            max_payload: self.max_payload,
            closed: self.closed.clone(),
        }
    }
}

struct WeakLink<P> {
    tx: mpsc::WeakUnboundedSender<Command<P>>,
    parser: P,
    max_payload: u64,
    closed: watch::Receiver<Option<DisconnectReason>>,
}

impl<P: Parse> WeakLink<P> {
    fn upgrade(&self) -> Option<Link<P>> {
        Some(Link {
            tx: self.tx.upgrade()?,
            parser: self.parser,
            max_payload: self.max_payload,
            closed: self.closed.clone(),
        })
    }
}

pub(crate) fn spawn<P: Parse + Send + Sync + 'static>(engine: Engine, parser: P) -> Link<P> {
    let (tx, rx) = mpsc::unbounded_channel();
    let (closed_tx, closed_rx) = watch::channel(None);
    let link = Link {
        tx,
        parser,
        max_payload: engine.open_packet().max_payload,
        closed: closed_rx,
    };
    let driver = Driver {
        link: link.downgrade(),
        parse_config: ParseConfig::default(),
        parser_state: ParserState::default(),
        sockets: HashMap::new(),
        outbox: Vec::new(),
    };
    tokio::spawn(driver.run(Box::pin(engine), rx, closed_tx));
    link
}

struct Driver<P> {
    link: WeakLink<P>,
    parse_config: ParseConfig,
    parser_state: ParserState,
    sockets: HashMap<Str, Entry<P>>,
    outbox: Vec<EioEvent>,
}

struct Entry<P> {
    socket: Arc<SocketState>,
    events: mpsc::UnboundedSender<SocketEvent<P>>,
    stage: Stage,
    acks: HashMap<i64, oneshot::Sender<Value>>,
}

enum Stage {
    Connecting(oneshot::Sender<Result<Arc<str>, ConnectError>>),
    Connected,
}

#[derive(Deserialize)]
struct ConnectReply {
    sid: String,
}

enum Next<P> {
    Engine(Option<Result<EioEvent, ClientError>>),
    Command(Option<Command<P>>),
    Heartbeat,
}

impl<P: Parse + Send + Sync + 'static> Driver<P> {
    async fn run(
        mut self,
        mut engine: Pin<Box<Engine>>,
        mut rx: mpsc::UnboundedReceiver<Command<P>>,
        closed: watch::Sender<Option<DisconnectReason>>,
    ) {
        let reason = self.drive(&mut engine, &mut rx).await;
        for (_, entry) in self.sockets.drain() {
            entry.end(reason.clone());
        }
        if reason == DisconnectReason::ClientClose {
            time::timeout(CLOSE_TIMEOUT, engine.close()).await.ok();
        }
        closed.send_replace(Some(reason));
        drop(rx);
    }

    async fn drive(
        &mut self,
        engine: &mut Pin<Box<Engine>>,
        rx: &mut mpsc::UnboundedReceiver<Command<P>>,
    ) -> DisconnectReason {
        loop {
            let deadline = time::Instant::from_std(engine.heartbeat_deadline());
            let next = tokio::select! {
                event = engine.next() => Next::Engine(event),
                command = rx.recv() => Next::Command(command),
                () = time::sleep_until(deadline) => Next::Heartbeat,
            };
            let handled = match next {
                Next::Engine(Some(Ok(event))) => self.on_engine_event(event),
                Next::Engine(Some(Err(err))) if err.kind() == ClientErrorKind::InvalidPacket => {
                    Ok(())
                }
                Next::Engine(Some(Err(err))) => Err(engine_reason(&err)),
                Next::Engine(None) => Err(DisconnectReason::TransportClose),
                Next::Command(Some(command)) => self.on_command(command),
                Next::Command(None) => Err(DisconnectReason::ClientClose),
                Next::Heartbeat => Ok(()),
            };
            if let Err(reason) = handled {
                return reason;
            }
            if let Err(reason) = self.flush(engine).await {
                return reason;
            }
        }
    }

    async fn flush(&mut self, engine: &mut Pin<Box<Engine>>) -> Result<(), DisconnectReason> {
        if self.outbox.is_empty() {
            return Ok(());
        }
        for packet in self.outbox.drain(..) {
            engine.feed(packet).await.map_err(|e| engine_reason(&e))?;
        }
        engine.flush().await.map_err(|e| engine_reason(&e))
    }

    fn on_engine_event(&mut self, event: EioEvent) -> Result<(), DisconnectReason> {
        let parser = self.link.parser;
        let decoded = match event {
            EioEvent::Message(data) => {
                parser.decode_str(&self.parse_config, &self.parser_state, data)
            }
            EioEvent::Binary(data) => {
                parser.decode_bin(&self.parse_config, &self.parser_state, data)
            }
            EioEvent::Disconnect => return Err(DisconnectReason::ServerClose),
            EioEvent::Connect(_) | EioEvent::Upgrade(_) => return Ok(()),
        };
        match decoded {
            Ok(packet) if packet.inner.is_binary() => Err(DisconnectReason::BinaryPacket),
            Ok(packet) => {
                self.on_packet(packet);
                Ok(())
            }
            Err(ParseError::NeedsMoreBinaryData | ParseError::UnexpectedBinaryPacket) => {
                Err(DisconnectReason::BinaryPacket)
            }
            Err(err) => Err(DisconnectReason::ParseError(err.to_string())),
        }
    }

    fn on_packet(&mut self, packet: Packet) {
        let parser = self.link.parser;
        let ns = packet.ns;
        let Some(entry) = self.sockets.get_mut(&ns) else {
            return;
        };
        let connected = matches!(entry.stage, Stage::Connected);
        match (packet.inner, connected) {
            (PacketData::Connect(value), false) => {
                let Stage::Connecting(reply) = mem::replace(&mut entry.stage, Stage::Connected)
                else {
                    unreachable!("the entry is connecting");
                };
                let sid = parser
                    .decode_default::<ConnectReply>(value.as_ref())
                    .map(|reply| Arc::<str>::from(reply.sid))
                    .map_err(|_| ConnectError::InvalidReply);
                let accepted = sid.is_ok();
                entry.socket.connected.store(accepted, Ordering::Release);
                let delivered = reply.send(sid).is_ok();
                if !accepted {
                    self.sockets.remove(&ns);
                } else if !delivered {
                    entry.socket.connected.store(false, Ordering::Release);
                    self.sockets.remove(&ns);
                    self.queue_disconnect(ns);
                }
            }
            (PacketData::ConnectError(message), false) => {
                if let Some(Entry {
                    stage: Stage::Connecting(reply),
                    ..
                }) = self.sockets.remove(&ns)
                {
                    reply.send(Err(ConnectError::Refused(message))).ok();
                }
            }
            (PacketData::Disconnect, _) => {
                if let Some(entry) = self.sockets.remove(&ns) {
                    entry.end(DisconnectReason::ServerDisconnect);
                }
            }
            (PacketData::Event(value, ack), true) => {
                let Ok(name) = parser.read_event(&value).map(str::to_owned) else {
                    return;
                };
                let ack = ack.and_then(|id| {
                    Some(AckSender::new(
                        self.link.upgrade()?,
                        entry.socket.clone(),
                        id,
                    ))
                });
                let args = Args::new(value, parser);
                entry
                    .events
                    .send(SocketEvent::Event { name, args, ack })
                    .ok();
            }
            (PacketData::EventAck(value, id), true) => {
                if let Some(reply) = entry.acks.remove(&id) {
                    reply.send(value).ok();
                }
            }
            _ => {}
        }
    }

    fn on_command(&mut self, command: Command<P>) -> Result<(), DisconnectReason> {
        match command {
            Command::Connect {
                socket,
                packet,
                events,
                reply,
            } => {
                if self.sockets.contains_key(&socket.ns) {
                    let ns = socket.ns.to_string();
                    reply.send(Err(ConnectError::AlreadyConnected(ns))).ok();
                } else {
                    let entry = Entry {
                        socket: socket.clone(),
                        events,
                        stage: Stage::Connecting(reply),
                        acks: HashMap::new(),
                    };
                    self.sockets.insert(socket.ns.clone(), entry);
                    self.outbox.push(packet);
                }
            }
            Command::Send { socket, packet } => {
                if self.live(&socket).is_some() {
                    self.outbox.push(packet);
                }
            }
            Command::Ack {
                socket,
                id,
                packet,
                reply,
            } => {
                if let Some(entry) = self.live(&socket) {
                    entry.acks.retain(|_, reply| !reply.is_closed());
                    entry.acks.insert(id, reply);
                    self.outbox.push(packet);
                }
            }
            Command::Disconnect { socket } => {
                let owned = self
                    .sockets
                    .get(&socket.ns)
                    .is_some_and(|entry| Arc::ptr_eq(&entry.socket, &socket));
                if let Some(entry) = owned.then(|| self.sockets.remove(&socket.ns)).flatten() {
                    self.queue_disconnect(socket.ns.clone());
                    entry.end(DisconnectReason::ClientDisconnect);
                }
            }
            Command::Close => return Err(DisconnectReason::ClientClose),
        }
        Ok(())
    }

    fn live(&mut self, socket: &Arc<SocketState>) -> Option<&mut Entry<P>> {
        self.sockets.get_mut(&socket.ns).filter(|entry| {
            Arc::ptr_eq(&entry.socket, socket) && matches!(entry.stage, Stage::Connected)
        })
    }

    fn queue_disconnect(&mut self, ns: Str) {
        let packet = Packet::disconnect(ns);
        if let Ok(packet) = packet::encode(self.link.parser, self.link.max_payload, packet) {
            self.outbox.push(packet);
        }
    }
}

impl<P> Entry<P> {
    fn end(self, reason: DisconnectReason) {
        self.socket.connected.store(false, Ordering::Release);
        match self.stage {
            Stage::Connecting(reply) => {
                reply.send(Err(ConnectError::Closed(reason))).ok();
            }
            Stage::Connected => {
                self.events.send(SocketEvent::Disconnected(reason)).ok();
            }
        }
    }
}

fn engine_reason(err: &ClientError) -> DisconnectReason {
    match err.kind() {
        ClientErrorKind::HeartbeatTimeout => DisconnectReason::PingTimeout,
        ClientErrorKind::TransportClosed => DisconnectReason::TransportClose,
        _ => DisconnectReason::TransportError(describe(err)),
    }
}
