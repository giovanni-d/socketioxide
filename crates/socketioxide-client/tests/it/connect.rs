use std::{fmt, sync::atomic::Ordering};

use serde_json::{Value, json};
use socketioxide::{
    extract::{AckSender, Data, SocketRef, TryData},
    handler::ConnectHandler,
};
use socketioxide_client::{Client, ConnectError};

use crate::fixture::{Server, join, within};

#[derive(Debug)]
struct Unauthorized;

impl fmt::Display for Unauthorized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid token")
    }
}

async fn authenticate(TryData(auth): TryData<Value>) -> Result<(), Unauthorized> {
    match auth {
        Ok(auth) if auth["token"] == "good" => Ok(()),
        _ => Err(Unauthorized),
    }
}

async fn control_server() -> Server {
    let server = Server::default().await;
    server.io.ns(
        "/control",
        {
            async |socket: SocketRef| {
                socket.on("query", async |Data::<Value>(data), ack: AckSender| {
                    ack.send(&data).ok();
                });
            }
        }
        .with(authenticate),
    );
    server
}

#[tokio::test]
async fn auth_accepted_by_middleware() {
    let server = control_server().await;
    let client = server.client().await;

    let (control, _events) = within(
        client
            .socket("/control")
            .auth(json!({ "token": "good" }))
            .connect(),
    )
    .await
    .unwrap();

    assert_eq!(control.ns(), "/control");
    assert!(!control.id().is_empty());
    assert!(control.connected());
    let reply: Value = within(control.emit_with_ack("query", &json!(1)))
        .await
        .unwrap();
    assert_eq!(reply, json!(1));
}

#[tokio::test]
async fn auth_refused_by_middleware() {
    let server = control_server().await;
    let client = server.client().await;

    let refused = within(
        client
            .socket("/control")
            .auth(json!({ "token": "bad" }))
            .connect(),
    )
    .await;
    assert!(
        matches!(&refused, Err(ConnectError::Refused(message)) if message == "invalid token"),
        "{refused:?}"
    );

    let refused = within(client.socket("/control").connect()).await;
    assert!(
        matches!(&refused, Err(ConnectError::Refused(_))),
        "{refused:?}"
    );

    let accepted = within(
        client
            .socket("/control")
            .auth(json!({ "token": "good" }))
            .connect(),
    )
    .await;
    assert!(accepted.is_ok(), "{accepted:?}");
}

#[tokio::test]
async fn unknown_namespace_is_refused() {
    let server = Server::default().await;
    let client = server.client().await;

    let refused = within(client.socket("/missing").connect()).await;
    assert!(
        matches!(&refused, Err(ConnectError::Refused(_))),
        "{refused:?}"
    );
}

#[tokio::test]
async fn namespace_is_connected_once_per_client() {
    let server = Server::default().await;
    server.io.ns("/control", async |_: SocketRef| {});
    let client = server.client().await;

    let (_control, _events) = join(&client, "control").await;
    let again = within(client.socket("/control").connect()).await;
    assert!(
        matches!(&again, Err(ConnectError::AlreadyConnected(ns)) if ns == "/control"),
        "{again:?}"
    );
}

#[tokio::test]
async fn namespaces_share_one_websocket() {
    const NAMESPACES: [&str; 4] = ["/control", "/terminal", "/voice", "/generation"];
    let server = Server::default().await;
    for ns in NAMESPACES {
        server.io.ns(ns, async |socket: SocketRef| {
            socket.on("query", async |socket: SocketRef, ack: AckSender| {
                ack.send(socket.ns()).ok();
            });
        });
    }
    let client = server.client().await;

    let mut sockets = Vec::new();
    for ns in NAMESPACES {
        sockets.push(join(&client, ns).await);
    }
    for (socket, _) in &sockets {
        let reply: String = within(socket.emit_with_ack("query", &())).await.unwrap();
        assert_eq!(reply, socket.ns());
    }
    assert_eq!(server.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn engine_connect_failures() {
    let refused = Client::builder("ftp://localhost:3000").connect().await;
    assert!(
        matches!(&refused, Err(ConnectError::UnsupportedScheme(scheme)) if scheme == "ftp"),
        "{refused:?}"
    );

    #[cfg(not(feature = "rustls"))]
    {
        let refused = Client::builder("wss://localhost:3000").connect().await;
        assert!(
            matches!(&refused, Err(ConnectError::UnsupportedScheme(scheme)) if scheme == "wss"),
            "{refused:?}"
        );
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    drop(listener);
    let refused = within(Client::builder(url).connect()).await;
    assert!(
        matches!(&refused, Err(ConnectError::Engine(_))),
        "{refused:?}"
    );
}

#[tokio::test]
async fn custom_engine_path() {
    let server = Server::start(socketioxide::SocketIo::builder().req_path("/custom")).await;
    server.io.ns("/", async |_: SocketRef| {});

    let client = within(Client::builder(&server.url).path("/custom/").connect())
        .await
        .unwrap();
    let (socket, _events) = join(&client, "/").await;
    assert!(socket.connected());
}
