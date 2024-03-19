use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::mqttbytes::{self, v4::*};
use crate::{Incoming, StateError};
use std::time::Duration;

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Frame MQTT packets from network connection
    framed: Framed<Box<dyn AsyncReadWrite>, Codec>,
    /// Time within which network operations should complete
    timeout: Duration,
}

impl Network {
    pub fn new(
        socket: impl AsyncReadWrite + 'static,
        max_incoming_size: usize,
        max_outgoing_size: usize,
        timeout: Duration,
    ) -> Network {
        let socket = Box::new(socket) as Box<dyn AsyncReadWrite>;
        let codec = Codec {
            max_incoming_size,
            max_outgoing_size,
        };
        let framed = Framed::new(socket, codec);

        Network { framed, timeout }
    }

    pub async fn read(&mut self) -> Result<Incoming, StateError> {
        match self.framed.next().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(mqttbytes::Error::InsufficientBytes(_))) | None => unreachable!(),
            Some(Err(e)) => Err(StateError::Deserialization(e)),
        }
    }

    pub async fn send(&mut self, packet: Packet) -> Result<(), crate::state::StateError> {
        match timeout(self.timeout, self.framed.send(packet)).await {
            Ok(inner) => inner.map_err(Into::into),
            Err(_) => Err(StateError::FlushTimeout),
        }
    }
}

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
