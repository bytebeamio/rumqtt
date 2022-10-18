use serde::{self, Deserialize, Serialize};
use serde_json::Value;
use tokio_util::codec::{Decoder, Encoder};
use websocket_codec::MessageCodec;

use crate::protocol::Error;

use super::{Packet, Protocol};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum Outgoing {
    #[serde(alias = "connack")]
    ConnAck { status: bool },
    #[serde(alias = "publish")]
    Publish { topic: String, data: Vec<Value> },
    #[serde(alias = "pong")]
    Pong { pong: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type")]
pub enum Incoming {
    #[serde(alias = "connect")]
    Connect { client_id: String, keep_alive: u16 },
    #[serde(alias = "shadow")]
    Shadow { filter: String },
    #[serde(alias = "ping")]
    Ping { ping: bool },
    #[serde(alias = "publish")]
    Publish { topic: String, data: Vec<Value> },
}

#[derive(Clone)]
pub struct Ws {
    pub codec: MessageCodec,
}

impl Protocol for Ws {
    fn read_mut(
        &mut self,
        stream: &mut bytes::BytesMut,
        max_size: usize,
    ) -> Result<super::Packet, Error> {
        let message = match self.codec.decode(stream) {
            Ok(Some(message)) => message,
            Ok(None) => return Err(Error::InsufficientBytes(1)),
            Err(e) => {
                dbg!(e);
                unimplemented!();
            }
        };

        dbg!(message);
        unimplemented!()
    }

    fn write(&self, packet: Packet, write: &mut bytes::BytesMut) -> Result<usize, super::Error> {
        todo!()
    }
}
