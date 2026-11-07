#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

mod client;
mod driver;
mod errors;
mod packet;
mod socket;
#[cfg(feature = "rustls")]
mod tls;
mod transport;

pub use client::{Client, ClientBuilder};
pub use engineioxide_client::Sid;
pub use errors::{AckError, ConnectError, DisconnectReason, SendError};
#[cfg(feature = "rustls")]
pub use rustls;
pub use socket::{AckRequest, AckSender, Args, Events, Socket, SocketBuilder, SocketEvent};
pub use socketioxide_core::parser::{Parse, ParserError};
pub use socketioxide_parser_common::CommonParser;
