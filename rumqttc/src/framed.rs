use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::mqttbytes::{self, v4::*};
use crate::{Incoming, MqttState, StateError};
use std::time::Duration;

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Frame MQTT packets from network connection
    framed: Framed<Box<dyn AsyncReadWrite>, Codec>,
    /// Maximum readv count
    max_readb_count: usize,
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

        Network {
            framed,
            max_readb_count: 10,
            timeout,
        }
    }

    pub async fn read(&mut self) -> Result<Incoming, StateError> {
        match self.framed.next().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(mqttbytes::Error::InsufficientBytes(_))) | None => unreachable!(),
            Some(Err(e)) => Err(StateError::Deserialization(e)),
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn readb(&mut self, state: &mut MqttState) -> Result<(), StateError> {
        let mut count = 0;
        loop {
            match self.framed.next().await {
                Some(Ok(packet)) => {
                    if let Some(packet) = state.handle_incoming_packet(packet)? {
                        self.send(packet).await?;
                    }

                    count += 1;
                    if count >= self.max_readb_count {
                        break;
                    }
                }
                // If some packets are already framed, return those
                Some(Err(mqttbytes::Error::InsufficientBytes(_))) | None if count > 0 => break,
                // NOTE: read atleast 1 packet
                Some(Err(mqttbytes::Error::InsufficientBytes(_))) | None => unreachable!(),
                Some(Err(e)) => return Err(StateError::Deserialization(e)),
            };
        }

        Ok(())
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
