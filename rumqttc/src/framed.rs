use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::mqttbytes::{self, v4::*};
use crate::{Incoming, MqttState, StateError};

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Frame MQTT packets from network connection
    framed: Framed<Box<dyn AsyncReadWrite>, Codec>,
    /// Maximum readv count
    max_readb_count: usize,
}

impl Network {
    pub fn new(
        socket: impl AsyncReadWrite + 'static,
        max_incoming_size: usize,
        max_outgoing_size: usize,
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
        }
    }

    /// Reads and returns a single packet from network
    pub async fn read(&mut self) -> Result<Incoming, StateError> {
        match self.framed.next().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(mqttbytes::Error::InsufficientBytes(_))) => unreachable!(),
            Some(Err(e)) => Err(StateError::Deserialization(e)),
            None => Err(StateError::ConnectionAborted),
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn readb(&mut self, state: &mut MqttState) -> Result<(), StateError> {
        // wait for the first read
        let mut res = self.framed.next().await;
        let mut count = 1;
        loop {
            match res {
                Some(Ok(packet)) => {
                    if let Some(outgoing) = state.handle_incoming_packet(packet)? {
                        self.write(outgoing).await?;
                    }

                    count += 1;
                    if count >= self.max_readb_count {
                        break;
                    }
                }
                Some(Err(mqttbytes::Error::InsufficientBytes(_))) => unreachable!(),
                Some(Err(e)) => return Err(StateError::Deserialization(e)),
                None => return Err(StateError::ConnectionAborted),
            }
            // do not wait for subsequent reads
            match self.framed.next().now_or_never() {
                Some(r) => res = r,
                _ => break,
            };
        }

        Ok(())
    }

    /// Serializes packet into write buffer
    pub async fn write(&mut self, packet: Packet) -> Result<(), StateError> {
        self.framed
            .feed(packet)
            .await
            .map_err(StateError::Deserialization)
    }

    pub async fn flush(&mut self) -> Result<(), crate::state::StateError> {
        self.framed
            .flush()
            .await
            .map_err(StateError::Deserialization)
    }
}

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
