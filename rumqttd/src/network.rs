use bytes::BytesMut;
use mqtt4bytes::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use std::collections::VecDeque;
use std::io::{self, ErrorKind};

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Socket for IO
    socket: Box<dyn N>,
    /// Buffered reads
    read: BytesMut,
    /// Buffered writes
    write: BytesMut,
    /// Maximum packet size
    max_incoming_size: usize,
    /// Maximum readv count
    max_readb_count: usize,
}

impl Network {
    pub fn new(socket: impl N + 'static, max_incoming_size: usize) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            read: BytesMut::with_capacity(10 * 1024),
            write: BytesMut::with_capacity(10 * 1024),
            max_incoming_size,
            max_readb_count: 10,
        }
    }

    /// Reads more than 'count' bytes into self.read buffer
    async fn read_bytes(&mut self, required: usize) -> io::Result<usize> {
        let mut total_read = 0;
        loop {
            let read = self.socket.read_buf(&mut self.read).await?;
            if 0 == read {
                return if self.read.is_empty() {
                    Err(io::Error::new(
                        ErrorKind::ConnectionAborted,
                        "connection closed by peer",
                    ))
                } else {
                    Err(io::Error::new(
                        ErrorKind::ConnectionReset,
                        "connection reset by peer",
                    ))
                };
            }

            total_read += read;
            if total_read >= required {
                return Ok(total_read);
            }
        }
    }

    pub async fn read(&mut self) -> Result<Packet, io::Error> {
        loop {
            let required = match mqtt_read(&mut self.read, self.max_incoming_size) {
                Ok(packet) => return Ok(packet),
                Err(Error::InsufficientBytes(required)) => required,
                Err(e) => return Err(io::Error::new(ErrorKind::InvalidData, e.to_string())),
            };

            // read more packets until a frame can be created. This function
            // blocks until a frame can be created. Use this in a select! branch
            self.read_bytes(required).await?;
        }
    }

    pub async fn read_connect(&mut self) -> io::Result<Connect> {
        let packet = self.read().await?;

        match packet {
            Packet::Connect(connect) => Ok(connect),
            packet => {
                let error = format!("Expecting connack. Received = {:?}", packet);
                Err(io::Error::new(io::ErrorKind::InvalidData, error))
            }
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn readb(&mut self, out: &mut VecDeque<Packet>) -> Result<(), io::Error> {
        loop {
            match mqtt_read(&mut self.read, self.max_incoming_size) {
                // Error on connect packet after the connection is established
                Ok(Packet::Connect(_)) => {
                    return Err(io::Error::new(ErrorKind::InvalidData, "Duplicate connect"));
                }
                // Connack is an invalid packet for a broker
                Ok(Packet::ConnAck(_)) => {
                    return Err(io::Error::new(ErrorKind::InvalidData, "Client connack"));
                }
                // Store packet and return after enough packets are accumulated
                Ok(packet) => {
                    out.push_back(packet);
                    if out.len() >= self.max_readb_count {
                        return Ok(());
                    }
                }
                // Wait for more bytes until a frame can be created or return with existing
                Err(Error::InsufficientBytes(required)) => {
                    // If some packets are already framed, return those instead
                    // of blocking until max readb count
                    if !out.is_empty() {
                        return Ok(());
                    }

                    self.read_bytes(required).await?;
                }
                Err(e) => return Err(io::Error::new(ErrorKind::InvalidData, e.to_string())),
            };
        }
    }

    #[inline]
    fn write_fill(&mut self, request: Packet) -> Result<usize, Error> {
        let size = match request {
            Packet::Publish(packet) => packet.write(&mut self.write)?,
            Packet::PubRel(packet) => packet.write(&mut self.write)?,
            Packet::PingResp => PingResp.write(&mut self.write)?,
            Packet::Subscribe(packet) => packet.write(&mut self.write)?,
            Packet::SubAck(packet) => packet.write(&mut self.write)?,
            Packet::Unsubscribe(packet) => packet.write(&mut self.write)?,
            Packet::UnsubAck(packet) => packet.write(&mut self.write)?,
            Packet::Disconnect => Disconnect.write(&mut self.write)?,
            Packet::PubAck(packet) => packet.write(&mut self.write)?,
            Packet::PubRec(packet) => packet.write(&mut self.write)?,
            Packet::PubComp(packet) => packet.write(&mut self.write)?,
            packet => unimplemented!("{:?}", packet),
        };

        Ok(size)
    }

    pub async fn connack(&mut self, connack: ConnAck) -> Result<usize, io::Error> {
        let len = match connack.write(&mut self.write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        self.flush().await?;
        Ok(len)
    }

    pub fn fill(&mut self, request: Packet) -> Result<(), io::Error> {
        if let Err(e) = self.write_fill(request) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
        };

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        if self.write.is_empty() {
            return Ok(());
        }

        self.socket.write_all(&self.write[..]).await?;
        self.write.clear();
        Ok(())
    }
}

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
