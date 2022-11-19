use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use std::{
    collections::VecDeque,
    io::{self, ErrorKind},
};
use tokio::time::{error::Elapsed, Duration};

use crate::{
    protocol::{self, Packet, Protocol},
    router::Ack,
    Notification,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O = {0}")]
    Io(#[from] io::Error),
    #[error("Invalid data = {0}")]
    Protocol(#[from] protocol::Error),
    #[error["Keep alive timeout"]]
    KeepAlive(#[from] Elapsed),
}

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network<P> {
    /// Socket for IO
    socket: Box<dyn N>,
    /// Buffered reads
    read: BytesMut,
    /// Buffered writes
    write: BytesMut,
    /// Maximum packet size
    max_incoming_size: usize,
    /// Maximum connection buffer count. TODO: Change this to use bytes for deterministicness
    max_connection_buffer_len: usize,
    /// Keep alive timeout
    keepalive: Duration,
    /// Protocol
    protocol: P,
}

impl<P: Protocol> Network<P> {
    pub fn new(
        socket: Box<dyn N>,
        max_incoming_size: usize,
        max_connection_buffer_len: usize,
        protocol: P,
    ) -> Network<P> {
        Network {
            socket,
            read: BytesMut::with_capacity(10 * 1024),
            write: BytesMut::with_capacity(10 * 1024),
            max_incoming_size,
            max_connection_buffer_len,
            keepalive: Duration::from_secs(0),
            protocol,
        }
    }

    pub fn set_keepalive(&mut self, keepalive: u16) {
        let keepalive = Duration::from_secs(keepalive as u64);
        self.keepalive = keepalive + keepalive.mul_f32(0.5);
    }

    /// Reads more than 'required' bytes to frame a packet into self.read buffer
    async fn read_bytes(&mut self, required: usize) -> io::Result<usize> {
        // TODO: Fix this cancellation bug and write unit test
        let mut total_read = 0;
        loop {
            let read = self.socket.read_buf(&mut self.read).await?;
            if 0 == read {
                let error = if self.read.is_empty() {
                    io::Error::new(ErrorKind::ConnectionAborted, "connection closed by peer")
                } else {
                    io::Error::new(ErrorKind::ConnectionReset, "connection reset by peer")
                };

                return Err(error);
            }

            total_read += read;
            if total_read >= required {
                return Ok(total_read);
            }
        }
    }

    /// Waits on network for 1 packet
    pub async fn read(&mut self) -> Result<Packet, io::Error> {
        loop {
            let required = match Protocol::read_mut(
                &mut self.protocol,
                &mut self.read,
                self.max_incoming_size,
            ) {
                Ok(packet) => return Ok(packet),
                Err(protocol::Error::InsufficientBytes(required)) => required,
                Err(e) => return Err(io::Error::new(ErrorKind::InvalidData, e.to_string())),
            };

            // read more packets until a frame can be created. This function
            // blocks until a frame can be created. Use this in a select! branch
            self.read_bytes(required).await?;
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub fn readv(&mut self, packets: &mut VecDeque<Packet>) -> Result<usize, Error> {
        loop {
            match self
                .protocol
                .read_mut(&mut self.read, self.max_incoming_size)
            {
                Ok(packet) => {
                    packets.push_back(packet);
                    let connection_buffer_length = packets.len();
                    if connection_buffer_length >= self.max_connection_buffer_len {
                        return Ok(connection_buffer_length);
                    }
                }
                Err(protocol::Error::InsufficientBytes(_)) => return Ok(packets.len()),
                Err(e) => return Err(io::Error::new(ErrorKind::InvalidData, e.to_string()).into()),
            }
        }
    }

    pub async fn write(&mut self, notification: Notification) -> Result<bool, Error> {
        let mut unscheduled = false;
        let packet_or_unscheduled = extract_packet_from_notification(notification);
        if let Some(packet) = packet_or_unscheduled {
            Protocol::write(&self.protocol, packet, &mut self.write)?;
        } else {
            unscheduled = true;
        }
        self.socket.write_all(&self.write).await?;
        self.write.clear();
        Ok(unscheduled)
    }

    pub async fn writev(
        &mut self,
        notifications: &mut VecDeque<Notification>,
    ) -> Result<bool, Error> {
        let mut o = false;
        for notification in notifications.drain(..) {
            let packet_or_unscheduled = extract_packet_from_notification(notification);
            if let Some(packet) = packet_or_unscheduled {
                Protocol::write(&self.protocol, packet, &mut self.write)?;
            } else {
                o = true
            }
        }
        self.socket.write_all(&self.write).await?;
        self.write.clear();
        Ok(o)
    }
}

// We either get a Packet to write to buffer or we unschedule which is represented as `None`
fn extract_packet_from_notification(notification: Notification) -> Option<Packet> {
    let packet: Packet;
    match notification {
        Notification::Forward(forward) => {
            packet = Packet::Publish(forward.publish, None);
        }
        Notification::DeviceAck(ack) => match ack {
            Ack::ConnAck(_, connack) => {
                packet = Packet::ConnAck(connack, None);
            }
            Ack::PubAck(puback) => {
                packet = Packet::PubAck(puback, None);
            }
            Ack::SubAck(suback) => {
                packet = Packet::SubAck(suback, None);
            }
            Ack::PingResp(pingresp) => {
                packet = Packet::PingResp(pingresp);
            }
            Ack::PubRec(pubrec) => {
                packet = Packet::PubRec(pubrec, None);
            }
            Ack::PubRel(pubrel) => {
                packet = Packet::PubRel(pubrel, None);
            }
            Ack::PubComp(pubcomp) => {
                packet = Packet::PubComp(pubcomp, None);
            }
            Ack::UnsubAck(unsuback) => {
                packet = Packet::UnsubAck(unsuback, None);
            }
            _ => unimplemented!(),
        },
        Notification::Unschedule => return None,
        v => unreachable!("{:?}", v),
    }
    Some(packet)
}

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
