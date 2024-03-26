use futures_util::SinkExt;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::framed::AsyncReadWrite;

use super::mqttbytes::v5::Packet;
use super::{mqttbytes, Codec, Connect, Login, MqttOptions};
use super::{Incoming, StateError};

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Frame MQTT packets from network connection
    framed: Framed<Box<dyn AsyncReadWrite>, Codec>,
}
impl Network {
    pub fn new(socket: impl AsyncReadWrite + 'static, max_incoming_size: Option<u32>) -> Network {
        let socket = Box::new(socket) as Box<dyn AsyncReadWrite>;
        let codec = Codec {
            max_incoming_size,
            max_outgoing_size: None,
        };
        let framed = Framed::new(socket, codec);

        Network { framed }
    }

    pub fn set_max_outgoing_size(&mut self, max_outgoing_size: Option<u32>) {
        self.framed.codec_mut().max_outgoing_size = max_outgoing_size;
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

    /// Serializes packet into write buffer
    pub async fn write(&mut self, packet: Packet) -> Result<(), StateError> {
        self.framed
            .feed(packet)
            .await
            .map_err(StateError::Deserialization)
    }

    /// Flush the outgoing sink
    pub async fn flush(&mut self) -> Result<(), StateError> {
        self.framed
            .flush()
            .await
            .map_err(StateError::Deserialization)
    }

    pub async fn connect(
        &mut self,
        connect: Connect,
        options: &MqttOptions,
    ) -> Result<(), StateError> {
        let last_will = options.last_will();
        let login = options.credentials();
        self.write(Packet::Connect(connect, last_will, login))
            .await?;

        self.flush().await
    }
}
