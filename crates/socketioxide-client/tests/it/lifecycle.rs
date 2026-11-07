use std::time::{Duration, Instant};

use serde_json::{Value, json};
use socketioxide::{
    SocketIo,
    extract::{AckSender, SocketRef},
    socket::DisconnectReason as ServerReason,
};
use socketioxide_client::{AckError, DisconnectReason, SendError};
use tokio::sync::mpsc;

use crate::fixture::{FakeServer, Server, disconnected, fake_join, join, quiet, within};

fn echo(io: &SocketIo, ns: &'static str, left: Option<mpsc::UnboundedSender<ServerReason>>) {
    io.ns(ns, async move |socket: SocketRef| {
        socket.on("query", async |ack: AckSender| {
            ack.send(&"pong").ok();
        });
        socket.on("leave", async |socket: SocketRef| {
            socket.disconnect().ok();
        });
        socket.on("hang", async |socket: SocketRef, _: AckSender| {
            socket.disconnect().ok();
        });
        if let Some(left) = left.clone() {
            socket.on_disconnect(async move |reason: ServerReason| {
                left.send(reason).ok();
            });
        }
    });
}

async fn alive(socket: &socketioxide_client::Socket) {
    let reply: String = within(socket.emit_with_ack("query", &())).await.unwrap();
    assert_eq!(reply, "pong");
}

#[tokio::test]
async fn heartbeat_keeps_the_connection() {
    let server = Server::start(
        SocketIo::builder()
            .ping_interval(Duration::from_millis(50))
            .ping_timeout(Duration::from_millis(50)),
    )
    .await;
    echo(&server.io, "/control", None);
    let client = server.client().await;
    let (control, mut events) = join(&client, "/control").await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    alive(&control).await;
    quiet(&mut events).await;
    let closed = tokio::time::timeout(Duration::from_millis(10), client.closed()).await;
    assert!(closed.is_err(), "{closed:?}");
}

#[tokio::test]
async fn missing_pings_end_the_client() {
    let fake = FakeServer::bind().await;
    let (client, mut ws) = fake.client(100, 100).await;
    let started = Instant::now();
    let (_control, mut control_events) = fake_join(&client, &mut ws, "/control").await;
    let (_terminal, mut terminal_events) = fake_join(&client, &mut ws, "/terminal").await;

    assert_eq!(
        disconnected(&mut control_events).await,
        DisconnectReason::PingTimeout
    );
    assert_eq!(
        disconnected(&mut terminal_events).await,
        DisconnectReason::PingTimeout
    );
    assert_eq!(within(client.closed()).await, DisconnectReason::PingTimeout);
    assert!(started.elapsed() >= Duration::from_millis(200));
    drop(ws);
}

#[tokio::test]
async fn server_namespace_disconnect_ends_only_that_socket() {
    let server = Server::default().await;
    echo(&server.io, "/control", None);
    echo(&server.io, "/terminal", None);
    let client = server.client().await;
    let (control, mut control_events) = join(&client, "/control").await;
    let (terminal, mut terminal_events) = join(&client, "/terminal").await;

    terminal.emit("leave", &()).unwrap();
    assert_eq!(
        disconnected(&mut terminal_events).await,
        DisconnectReason::ServerDisconnect
    );
    assert!(!terminal.connected());
    assert!(matches!(
        terminal.emit("query", &()),
        Err(SendError::Disconnected)
    ));

    alive(&control).await;
    quiet(&mut control_events).await;

    let (terminal, _events) = join(&client, "/terminal").await;
    alive(&terminal).await;
}

#[tokio::test]
async fn pending_acks_fail_when_the_socket_ends() {
    let server = Server::default().await;
    echo(&server.io, "/control", None);
    let client = server.client().await;
    let (control, mut events) = join(&client, "/control").await;

    let pending = within(
        control
            .emit_with_ack::<_, Value>("hang", &())
            .timeout(Duration::from_secs(20)),
    )
    .await;
    assert!(
        matches!(pending, Err(AckError::Disconnected)),
        "{pending:?}"
    );
    assert_eq!(
        disconnected(&mut events).await,
        DisconnectReason::ServerDisconnect
    );
}

#[tokio::test]
async fn pending_acks_fail_when_the_client_ends() {
    let fake = FakeServer::bind().await;
    let (client, mut ws) = fake.client(25000, 20000).await;
    let (control, _events) = fake_join(&client, &mut ws, "/control").await;

    let pending = control
        .emit_with_ack::<_, Value>("query", &json!(1))
        .timeout(Duration::from_secs(20));
    let pending = tokio::spawn(async move { pending.await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(ws);

    let pending = within(pending).await.unwrap();
    assert!(
        matches!(pending, Err(AckError::Disconnected)),
        "{pending:?}"
    );
}

#[tokio::test]
async fn client_namespace_disconnect() {
    let server = Server::default().await;
    let (left_tx, mut left) = mpsc::unbounded_channel();
    echo(&server.io, "/control", Some(left_tx));
    echo(&server.io, "/terminal", None);
    let client = server.client().await;
    let (control, mut control_events) = join(&client, "/control").await;
    let (terminal, _terminal_events) = join(&client, "/terminal").await;

    control.disconnect().unwrap();
    assert_eq!(
        disconnected(&mut control_events).await,
        DisconnectReason::ClientDisconnect
    );
    assert_eq!(
        within(left.recv()).await.unwrap(),
        ServerReason::ClientNSDisconnect
    );
    assert!(matches!(control.disconnect(), Err(SendError::Disconnected)));
    alive(&terminal).await;
}

#[tokio::test]
async fn server_shutdown_ends_every_socket_once() {
    let server = Server::default().await;
    echo(&server.io, "/control", None);
    echo(&server.io, "/terminal", None);
    let client = server.client().await;
    let (_control, mut control_events) = join(&client, "/control").await;
    let (_terminal, mut terminal_events) = join(&client, "/terminal").await;

    within(server.io.close()).await;

    assert_eq!(
        disconnected(&mut control_events).await,
        DisconnectReason::ServerClose
    );
    assert_eq!(
        disconnected(&mut terminal_events).await,
        DisconnectReason::ServerClose
    );
    assert_eq!(within(client.closed()).await, DisconnectReason::ServerClose);
}

#[tokio::test]
async fn lost_connection_ends_every_socket_once() {
    let fake = FakeServer::bind().await;
    let (client, mut ws) = fake.client(25000, 20000).await;
    let (control, mut control_events) = fake_join(&client, &mut ws, "/control").await;
    let (_terminal, mut terminal_events) = fake_join(&client, &mut ws, "/terminal").await;

    drop(ws);

    let lost = |reason: &DisconnectReason| {
        matches!(
            reason,
            DisconnectReason::TransportClose | DisconnectReason::TransportError(_)
        )
    };
    let reason = disconnected(&mut control_events).await;
    assert!(lost(&reason), "{reason:?}");
    assert_eq!(disconnected(&mut terminal_events).await, reason);
    assert_eq!(within(client.closed()).await, reason);
    assert!(matches!(
        control.emit("query", &()),
        Err(SendError::Disconnected)
    ));
}

#[tokio::test]
async fn client_close_ends_every_socket_once() {
    let server = Server::default().await;
    let (left_tx, mut left) = mpsc::unbounded_channel();
    echo(&server.io, "/control", Some(left_tx));
    echo(&server.io, "/terminal", None);
    let client = server.client().await;
    let (_control, mut control_events) = join(&client, "/control").await;
    let (_terminal, mut terminal_events) = join(&client, "/terminal").await;

    client.close();

    assert_eq!(
        disconnected(&mut control_events).await,
        DisconnectReason::ClientClose
    );
    assert_eq!(
        disconnected(&mut terminal_events).await,
        DisconnectReason::ClientClose
    );
    assert_eq!(within(client.closed()).await, DisconnectReason::ClientClose);
    within(left.recv()).await.unwrap();
}

#[tokio::test]
async fn dropping_every_handle_closes_the_websocket() {
    let server = Server::default().await;
    let (left_tx, mut left) = mpsc::unbounded_channel();
    echo(&server.io, "/control", Some(left_tx));
    let client = server.client().await;
    let (control, events) = join(&client, "/control").await;

    drop((client, control, events));
    within(left.recv()).await.unwrap();
}
