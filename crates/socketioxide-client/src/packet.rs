use engineioxide_client::EioEvent;
use socketioxide_core::{Str, Value, packet::Packet, parser::Parse};

use crate::errors::SendError;

pub(crate) fn event(ns: Str, value: Value, ack: Option<i64>) -> Packet {
    let mut packet = Packet::event(ns, value);
    if let Some(id) = ack {
        packet.inner.set_ack_id(id);
    }
    packet
}

pub(crate) fn encode<P: Parse>(
    parser: P,
    max_payload: u64,
    packet: Packet,
) -> Result<EioEvent, SendError> {
    if packet.inner.is_binary() {
        return Err(SendError::Binary);
    }
    let (size, event) = match parser.encode(packet) {
        Value::Str(data, _) => (data.len() + 1, EioEvent::Message(data)),
        Value::Bytes(data) => (data.len(), EioEvent::Binary(data)),
    };
    if size as u64 > max_payload {
        return Err(SendError::PayloadTooLarge {
            size,
            max: max_payload,
        });
    }
    Ok(event)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::{Value as Json, json};
    use socketioxide_core::{
        packet::PacketData,
        parser::{ParseConfig, ParserState},
    };
    use socketioxide_parser_common::CommonParser;

    use super::*;

    const NAMESPACES: [&str; 2] = ["/", "/control"];

    fn round_trip(packet: Packet) -> (String, Packet) {
        let EioEvent::Message(data) = encode(CommonParser, u64::MAX, packet).unwrap() else {
            panic!("the common parser encodes text packets");
        };
        let decoded = CommonParser
            .decode_str(
                &ParseConfig::default(),
                &ParserState::default(),
                data.clone(),
            )
            .unwrap();
        (data.into(), decoded)
    }

    fn prefix(ns: &str) -> String {
        if ns == "/" {
            String::new()
        } else {
            format!("{ns},")
        }
    }

    #[test]
    fn connect_with_auth() {
        for ns in NAMESPACES {
            let auth = CommonParser
                .encode_default(&json!({ "token": "abc" }))
                .unwrap();
            let (data, packet) = round_trip(Packet::connect(ns, Some(auth)));
            assert_eq!(data, format!("0{}{{\"token\":\"abc\"}}", prefix(ns)));
            assert_eq!(packet.ns, ns);
            let PacketData::Connect(value) = packet.inner else {
                panic!("expected a connect packet, got {:?}", packet.inner);
            };
            let auth: Json = CommonParser.decode_default(value.as_ref()).unwrap();
            assert_eq!(auth, json!({ "token": "abc" }));
        }
    }

    #[test]
    fn connect_without_auth() {
        for ns in NAMESPACES {
            let (data, packet) = round_trip(Packet::connect(ns, None));
            assert_eq!(data, format!("0{}", prefix(ns)));
            assert_eq!(packet, Packet::connect(ns, None));
        }
    }

    #[test]
    fn event_without_ack() {
        for ns in NAMESPACES {
            let value = CommonParser
                .encode_value(&json!({ "kind": "start" }), Some("command"))
                .unwrap();
            let (data, packet) = round_trip(event(ns.into(), value, None));
            assert_eq!(
                data,
                format!("2{}[\"command\",{{\"kind\":\"start\"}}]", prefix(ns))
            );
            assert_eq!(packet.ns, ns);
            let PacketData::Event(mut value, None) = packet.inner else {
                panic!("expected an event without ack, got {:?}", packet.inner);
            };
            assert_eq!(CommonParser.read_event(&value).unwrap(), "command");
            let data: Json = CommonParser.decode_value(&mut value, true).unwrap();
            assert_eq!(data, json!({ "kind": "start" }));
        }
    }

    #[test]
    fn event_with_ack() {
        for ns in NAMESPACES {
            let value = CommonParser
                .encode_value(&json!({ "kind": "status" }), Some("query"))
                .unwrap();
            let (data, packet) = round_trip(event(ns.into(), value, Some(7)));
            assert_eq!(
                data,
                format!("2{}7[\"query\",{{\"kind\":\"status\"}}]", prefix(ns))
            );
            assert_eq!(packet.ns, ns);
            let PacketData::Event(mut value, Some(7)) = packet.inner else {
                panic!("expected an event with ack 7, got {:?}", packet.inner);
            };
            assert_eq!(CommonParser.read_event(&value).unwrap(), "query");
            let data: Json = CommonParser.decode_value(&mut value, true).unwrap();
            assert_eq!(data, json!({ "kind": "status" }));
        }
    }

    #[test]
    fn ack() {
        for ns in NAMESPACES {
            let value = CommonParser.encode_value(&json!("ok"), None).unwrap();
            let (data, packet) = round_trip(Packet::ack(ns, value, 3));
            assert_eq!(data, format!("3{}3[\"ok\"]", prefix(ns)));
            assert_eq!(packet.ns, ns);
            let PacketData::EventAck(mut value, 3) = packet.inner else {
                panic!("expected an ack with id 3, got {:?}", packet.inner);
            };
            let data: String = CommonParser.decode_value(&mut value, false).unwrap();
            assert_eq!(data, "ok");
        }
    }

    #[test]
    fn disconnect() {
        for ns in NAMESPACES {
            let (data, packet) = round_trip(Packet::disconnect(ns));
            assert_eq!(data, format!("1{}", prefix(ns)));
            assert_eq!(packet, Packet::disconnect(ns));
        }
    }

    #[test]
    fn payload_limit() {
        let value = CommonParser.encode_value(&"abc", Some("e")).unwrap();
        let packet = event("/".into(), value, None);
        let size = "42[\"e\",\"abc\"]".len();
        assert!(encode(CommonParser, size as u64, packet.clone()).is_ok());
        assert!(matches!(
            encode(CommonParser, size as u64 - 1, packet),
            Err(SendError::PayloadTooLarge { size: s, max }) if s == size && max == size as u64 - 1
        ));
    }

    #[test]
    fn binary_attachments_are_refused() {
        let value = CommonParser
            .encode_value(&Bytes::from_static(&[1, 2, 3]), Some("e"))
            .unwrap();
        assert!(matches!(
            encode(CommonParser, u64::MAX, event("/".into(), value, None)),
            Err(SendError::Binary)
        ));
    }
}
