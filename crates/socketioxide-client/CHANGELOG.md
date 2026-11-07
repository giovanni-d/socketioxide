# socketioxide-client (unreleased)
* feat: Socket.IO v5 client over the Engine.IO v4 websocket of `engineioxide-client`, generic over the
`Parse` implementation (`CommonParser` by default).
* feat: one `Client` per websocket, a `Socket` per namespace with CONNECT auth, CONNECT_ERROR refusals,
`emit`, `emit_with_ack` with a timeout per call, and acknowledgements of server events through `AckSender`.
* feat: every socket ends with exactly one `SocketEvent::Disconnected`, and `Client::closed` resolves once
the websocket ends: close frame, end of stream, IO error, ping timeout or Engine.IO close.
* feat: packets over the server `maxPayload` are refused with `SendError::PayloadTooLarge`.
* feat: `wss://` and `https://` through rustls with the `ring` provider, behind the `rustls`,
`rustls-webpki-roots` and `rustls-native-roots` features, and `ClientBuilder::tls_config` for a custom
`rustls::ClientConfig`.
* feat: a websocket-only `engineioxide-client` flavor over `tokio-tungstenite`, without hyper.
