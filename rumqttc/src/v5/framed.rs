use futures_util::SinkExt;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::framed::AsyncReadWrite;

use super::mqttbytes::v5::Packet;
use super::{mqttbytes, Codec};
use super::{Incoming, StateError};
use std::time::Duration;

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Frame MQTT packets from network connection
    framed: Framed<Box<dyn AsyncReadWrite>, Codec>,
    /// Time within which network write operations should complete
    timeout: Duration,
    /// Capacity upto which buffering is good
    buffer_capacity: usize,
}

impl Network {
    pub fn new(
        socket: impl AsyncReadWrite + 'static,
        max_incoming_size: Option<usize>,
        max_outgoing_size: Option<usize>,
        timeout: Duration,
        buffer_capacity: usize,
    ) -> Network {
        let socket = Box::new(socket) as Box<dyn AsyncReadWrite>;
        let codec = Codec {
            max_incoming_size,
            max_outgoing_size,
        };
        let framed = Framed::with_capacity(socket, codec, buffer_capacity);

        Network {
            framed,
            timeout,
            buffer_capacity,
        }
    }

    pub async fn read(&mut self) -> Result<Incoming, StateError> {
        match self.framed.next().await {
            Some(Ok(packet)) => Ok(packet),
            Some(Err(mqttbytes::Error::InsufficientBytes(_))) => unreachable!(),
            Some(Err(e)) => Err(StateError::Deserialization(e)),
            None => Err(StateError::ConnectionClosed),
        }
    }

    /// Write packets into buffer, flushes `Connect`/`PingReq`/`PingResp` packets instantly,
    /// or on breaching buffer capacity
    pub async fn write(&mut self, packet: Packet) -> Result<(), StateError> {
        let packet_size = packet.size();
        let should_flush = match packet {
            Packet::Connect(..) | Packet::PingReq(_) | Packet::PingResp(_) => true,
            _ => false,
        };
        self.framed
            .feed(packet)
            .await
            .map_err(StateError::Deserialization)?;

        if should_flush || self.framed.write_buffer().len() + packet_size >= self.buffer_capacity {
            self.flush().await?;
        }

        Ok(())
    }

    /// Force flush all packets in buffer, reset count
    pub async fn flush(&mut self) -> Result<(), StateError> {
        match timeout(self.timeout, self.framed.flush()).await {
            Ok(inner) => inner.map_err(StateError::Deserialization),
            Err(e) => Err(StateError::Timeout(e)),
        }
    }
}
