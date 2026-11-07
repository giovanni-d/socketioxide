use std::{
    future::{Ready, ready},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use engineioxide_client::{
    Str, TransportType,
    flavors::{Flavor, PollingBody, WsMessage},
};
use futures_core::{Stream, future::BoxFuture};
use futures_util::{FutureExt, Sink};
use http::{Request, Response};
use http_body_util::Empty;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, Message},
};
use tower_service::Service;

#[derive(Debug, Clone, Default)]
pub(crate) struct WsFlavor {
    #[cfg(feature = "rustls")]
    tls: Option<std::sync::Arc<rustls::ClientConfig>>,
}

impl WsFlavor {
    #[cfg(feature = "rustls")]
    pub fn with_tls(tls: std::sync::Arc<rustls::ClientConfig>) -> Self {
        Self { tls: Some(tls) }
    }
}

impl Flavor for WsFlavor {
    const SUPPORTED_TRANSPORTS: &'static [TransportType] = &[TransportType::Websocket];
}

#[derive(Debug, thiserror::Error)]
#[error("the polling transport is not supported")]
pub(crate) struct PollingUnsupported;

impl Service<Request<PollingBody>> for WsFlavor {
    type Response = Response<Empty<Bytes>>;
    type Error = PollingUnsupported;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Err(PollingUnsupported))
    }

    fn call(&mut self, _: Request<PollingBody>) -> Self::Future {
        ready(Err(PollingUnsupported))
    }
}

impl Service<Request<()>> for WsFlavor {
    type Response = WebSocket;
    type Error = tungstenite::Error;
    type Future = BoxFuture<'static, Result<WebSocket, tungstenite::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<()>) -> Self::Future {
        let uri = req.into_parts().0.uri;
        #[cfg(feature = "rustls")]
        let connector = self.tls.clone().map(tokio_tungstenite::Connector::Rustls);
        async move {
            #[cfg(feature = "rustls")]
            let (inner, _) =
                tokio_tungstenite::connect_async_tls_with_config(uri, None, false, connector)
                    .await?;
            #[cfg(not(feature = "rustls"))]
            let (inner, _) = tokio_tungstenite::connect_async(uri).await?;
            Ok(WebSocket { inner })
        }
        .boxed()
    }
}

pub(crate) struct WebSocket {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Stream for WebSocket {
    type Item = Result<WsMessage, tungstenite::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let message = match std::task::ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(Message::Text(text))) => WsMessage::Text(Str::copy_from_slice(&text)),
                Some(Ok(Message::Binary(data))) => WsMessage::Binary(data),
                Some(Ok(Message::Close(_))) => WsMessage::Close,
                Some(Ok(_)) => continue,
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(None),
            };
            return Poll::Ready(Some(Ok(message)));
        }
    }
}

impl Sink<WsMessage> for WebSocket {
    type Error = tungstenite::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
        let message = match item {
            WsMessage::Text(text) => Message::text(String::from(text)),
            WsMessage::Binary(data) => Message::Binary(data),
            WsMessage::Close => Message::Close(None),
        };
        Pin::new(&mut self.inner).start_send(message)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}
