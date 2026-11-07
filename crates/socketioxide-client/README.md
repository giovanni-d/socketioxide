# socketioxide-client

A Socket.IO v5 client over Engine.IO v4 websockets, sharing its packet and parser types with
the socketioxide server. Several namespaces travel over one websocket.

```rust,no_run
use std::time::Duration;

use serde_json::{Value, json};
use socketioxide_client::{Client, SocketEvent};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder("ws://localhost:3000").connect().await?;

    let (control, mut events) = client
        .socket("/control")
        .auth(json!({ "token": "access" }))
        .connect()
        .await?;

    control.emit("command", &json!({ "kind": "start" }))?;
    let reply: Value = control
        .emit_with_ack("query", &json!({ "kind": "status" }))
        .timeout(Duration::from_secs(20))
        .await?;
    println!("{reply}");

    while let Some(event) = events.recv().await {
        match event {
            SocketEvent::Event { name, mut args, ack } => {
                let data: Value = args.decode()?;
                println!("{name}: {data}");
                if let Some(ack) = ack {
                    ack.send(&"ok")?;
                }
            }
            SocketEvent::Disconnected(reason) => {
                println!("disconnected: {reason}");
                break;
            }
        }
    }
    Ok(())
}
```

## TLS

`wss://` and `https://` URLs need one of these features, all built on rustls with the `ring` provider:

| Feature | Trusted certificates |
|---|---|
| `rustls-webpki-roots` | the Mozilla root certificates bundled in the binary, the same on every platform |
| `rustls-native-roots` | the certificate store of the operating system (Windows, macOS, Linux) |
| `rustls` | none: pass a `rustls::ClientConfig` with `ClientBuilder::tls_config` |

`ClientBuilder::tls_config` also overrides the roots of the first two features, for a private
certificate authority or a custom verifier.

Binary attachments and reconnection are out of scope.
