use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use socketioxide::{
    SocketIo,
    extract::{AckSender, Data, SocketRef},
};
use socketioxide_client::{AckError, DisconnectReason, SendError, SocketEvent};
use tokio::sync::mpsc;

use crate::fixture::{Server, disconnected, join, next_event, quiet, within};

fn control(io: &SocketIo, tx: mpsc::UnboundedSender<Value>) {
    io.ns("/control", async move |socket: SocketRef| {
        let commands = tx.clone();
        socket.on("command", async move |Data::<Value>(data)| {
            commands.send(data).ok();
        });
        socket.on("query", async |Data::<Value>(data), ack: AckSender| {
            ack.send(&json!({ "echo": data })).ok();
        });
        socket.on("never", async |_: AckSender| {});
        let answers = tx.clone();
        socket.on("ask", async move |socket: SocketRef| {
            let answer = socket
                .emit_with_ack::<_, Value>("ask", &json!({ "q": 1 }))
                .unwrap()
                .await;
            answers.send(answer.unwrap()).ok();
        });
        socket.on("subscribe", async |socket: SocketRef| {
            for egress in 1..=2 {
                socket.emit("message", &json!({ "egress": egress })).ok();
            }
        });
        socket.on("binary", async |socket: SocketRef| {
            socket.emit("blob", &Bytes::from_static(&[1, 2, 3])).ok();
        });
    });
}

async fn setup(builder: socketioxide::SocketIoBuilder) -> (Server, mpsc::UnboundedReceiver<Value>) {
    let server = Server::start(builder).await;
    let (tx, rx) = mpsc::unbounded_channel();
    control(&server.io, tx);
    (server, rx)
}

#[tokio::test]
async fn emit_reaches_the_server() {
    let (server, mut received) = setup(SocketIo::builder()).await;
    let client = server.client().await;
    let (control, _events) = join(&client, "/control").await;

    control
        .emit("command", &json!({ "kind": "start" }))
        .unwrap();
    assert_eq!(
        within(received.recv()).await.unwrap(),
        json!({ "kind": "start" })
    );
}

#[tokio::test]
async fn emit_with_ack_returns_the_reply() {
    let (server, _) = setup(SocketIo::builder()).await;
    let client = server.client().await;
    let (control, _events) = join(&client, "/control").await;

    for i in 0..3 {
        let reply: Value = within(
            control
                .emit_with_ack("query", &json!({ "id": i }))
                .timeout(Duration::from_secs(20)),
        )
        .await
        .unwrap();
        assert_eq!(reply, json!({ "echo": { "id": i } }));
    }
}

#[tokio::test]
async fn ack_times_out() {
    let (server, _) = setup(SocketIo::builder()).await;
    let client = server.client().await;
    let (control, _events) = join(&client, "/control").await;

    let missing = within(
        control
            .emit_with_ack::<_, Value>("never", &())
            .timeout(Duration::from_millis(100)),
    )
    .await;
    assert!(matches!(missing, Err(AckError::Timeout)), "{missing:?}");

    let reply: Value = within(control.emit_with_ack("query", &1)).await.unwrap();
    assert_eq!(reply, json!({ "echo": 1 }));
}

#[tokio::test]
async fn server_ack_request_is_answered() {
    let (server, mut answers) = setup(SocketIo::builder()).await;
    let client = server.client().await;
    let (control, mut events) = join(&client, "/control").await;

    control.emit("ask", &()).unwrap();
    let SocketEvent::Event {
        name,
        mut args,
        ack,
    } = next_event(&mut events).await
    else {
        panic!("expected an event");
    };
    assert_eq!(name, "ask");
    assert_eq!(args.decode::<Value>().unwrap(), json!({ "q": 1 }));
    ack.expect("the server waits for an ack")
        .send(&json!("yes"))
        .unwrap();

    assert_eq!(within(answers.recv()).await.unwrap(), json!("yes"));
}

#[tokio::test]
async fn server_events_arrive_in_order() {
    let (server, _) = setup(SocketIo::builder()).await;
    let client = server.client().await;
    let (control, mut events) = join(&client, "/control").await;

    control.emit("subscribe", &()).unwrap();
    for egress in 1..=2 {
        let SocketEvent::Event {
            name,
            mut args,
            ack,
        } = next_event(&mut events).await
        else {
            panic!("expected an event");
        };
        assert_eq!(name, "message");
        assert!(ack.is_none());
        assert_eq!(args.decode::<Value>().unwrap(), json!({ "egress": egress }));
    }
    quiet(&mut events).await;
}

#[tokio::test]
async fn packets_over_max_payload_are_refused() {
    let (server, mut received) = setup(SocketIo::builder().max_payload(1000)).await;
    let client = server.client().await;
    let (control, _events) = join(&client, "/control").await;

    let big = "x".repeat(2000);
    let refused = control.emit("command", &big);
    assert!(
        matches!(refused, Err(SendError::PayloadTooLarge { max: 1000, .. })),
        "{refused:?}"
    );
    let refused = within(control.emit_with_ack::<_, Value>("query", &big)).await;
    assert!(
        matches!(
            refused,
            Err(AckError::Send(SendError::PayloadTooLarge { max: 1000, .. }))
        ),
        "{refused:?}"
    );

    control.emit("command", &"small").unwrap();
    assert_eq!(within(received.recv()).await.unwrap(), json!("small"));
}

#[tokio::test]
async fn binary_attachments_are_refused() {
    let (server, _) = setup(SocketIo::builder()).await;
    let client = server.client().await;
    let (control, _events) = join(&client, "/control").await;

    let refused = control.emit("command", &Bytes::from_static(&[1, 2, 3]));
    assert!(matches!(refused, Err(SendError::Binary)), "{refused:?}");
    assert!(control.connected());
}

#[tokio::test]
async fn server_binary_packet_ends_the_client() {
    let (server, _) = setup(SocketIo::builder()).await;
    server.io.ns("/terminal", async |_: SocketRef| {});
    let client = server.client().await;
    let (control, mut control_events) = join(&client, "/control").await;
    let (_terminal, mut terminal_events) = join(&client, "/terminal").await;

    control.emit("binary", &()).unwrap();
    assert_eq!(
        disconnected(&mut control_events).await,
        DisconnectReason::BinaryPacket
    );
    assert_eq!(
        disconnected(&mut terminal_events).await,
        DisconnectReason::BinaryPacket
    );
    assert_eq!(
        within(client.closed()).await,
        DisconnectReason::BinaryPacket
    );
}
