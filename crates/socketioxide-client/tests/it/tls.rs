use socketioxide::{
    SocketIo,
    extract::{AckSender, Data, SocketRef},
};
use socketioxide_client::{Client, ConnectError};

use crate::fixture::{Server, join, tls, within};

async fn secure_server() -> Server {
    let server = Server::start_tls(SocketIo::builder()).await;
    server.io.ns("/control", async |socket: SocketRef| {
        socket.on("query", async |Data::<String>(data), ack: AckSender| {
            ack.send(&data).ok();
        });
    });
    server
}

#[tokio::test]
async fn wss_with_a_trusted_root() {
    let server = secure_server().await;

    for url in [server.url.clone(), server.url.replace("wss://", "https://")] {
        let client = within(
            Client::builder(&url)
                .tls_config(tls::client_config(true))
                .connect(),
        )
        .await
        .unwrap();
        let (control, _events) = join(&client, "/control").await;
        let reply: String = within(control.emit_with_ack("query", &"secure"))
            .await
            .unwrap();
        assert_eq!(reply, "secure");
    }
}

#[tokio::test]
async fn wss_rejects_an_untrusted_certificate() {
    let server = secure_server().await;

    let refused = within(
        Client::builder(&server.url)
            .tls_config(tls::client_config(false))
            .connect(),
    )
    .await;
    assert!(
        matches!(&refused, Err(ConnectError::Engine(_))),
        "{refused:?}"
    );
    assert!(
        format!("{refused:?}").contains("UnknownIssuer"),
        "{refused:?}"
    );
}

#[tokio::test]
#[cfg(any(feature = "rustls-webpki-roots", feature = "rustls-native-roots"))]
async fn default_roots_reject_a_private_ca() {
    let server = secure_server().await;

    let refused = within(Client::builder(&server.url).connect()).await;
    assert!(
        format!("{refused:?}").contains("UnknownIssuer"),
        "{refused:?}"
    );
}

#[tokio::test]
#[cfg(not(any(feature = "rustls-webpki-roots", feature = "rustls-native-roots")))]
async fn wss_without_roots_needs_a_config() {
    let refused = Client::builder("wss://localhost:1").connect().await;
    assert!(matches!(refused, Err(ConnectError::Tls(_))), "{refused:?}");
}
