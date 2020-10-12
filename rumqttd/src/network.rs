use bytes::BytesMut;
use mqtt4bytes::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use std::io;

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Socket for IO
    socket: Box<dyn N>,
    /// Pending bytes required to create next frame
    pending: usize,
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
            pending: 0,
            read: BytesMut::with_capacity(10 * 1024),
            write: BytesMut::with_capacity(10 * 1024),
            max_incoming_size,
            max_readb_count: 10,
        }
    }

    // TODO make this equivalent to `mqtt_read` to frame `Incoming` directly
    pub async fn read(&mut self) -> Result<Packet, io::Error> {
        loop {
            match mqtt_read(&mut self.read, self.max_incoming_size) {
                Ok(packet) => return Ok(packet),
                Err(Error::InsufficientBytes(required)) => self.pending = required,
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            }

            // read more packets until a frame can be created. This functions blocks until a frame
            // can be created. Use this in a select! branch
            let mut total_read = 0;
            loop {
                let read = self.read_fill().await?;
                total_read += read;
                if total_read >= self.pending {
                    self.pending = 0;
                    break;
                }
            }
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn readb(&mut self) -> Result<Vec<Packet>, io::Error> {
        let mut out = Vec::with_capacity(self.max_readb_count);
        loop {
            match mqtt_read(&mut self.read, self.max_incoming_size) {
                // Connection is explicitly handled by other methods. This read is used after establishing
                // the link. Ignore connection related packets
                Ok(Packet::Connect(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Not expecting connect",
                    ))
                }
                Ok(Packet::ConnAck(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Not expecting connack",
                    ))
                }
                Ok(packet) => {
                    out.push(packet);
                    if out.len() >= self.max_readb_count {
                        break;
                    }
                    continue;
                }
                Err(Error::InsufficientBytes(required)) => {
                    self.pending = required;
                    if !out.is_empty() {
                        break;
                    }
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            };

            let mut total_read = 0;
            loop {
                let read = self.read_fill().await?;
                total_read += read;
                if total_read >= self.pending {
                    self.pending = 0;
                    break;
                }
            }
        }

        Ok(out)
    }

    /// Fills the read buffer with more bytes
    async fn read_fill(&mut self) -> Result<usize, io::Error> {
        let read = self.socket.read_buf(&mut self.read).await?;
        if 0 == read {
            return if self.read.is_empty() {
                Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "connection closed by peer",
                ))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection reset by peer",
                ))
            };
        }

        Ok(read)
    }

    #[inline]
    fn write_fill(&mut self, request: Packet) -> Result<usize, Error> {
        // TODO Implement max_outgoing_packet_size using write size
        let size = match request {
            Packet::Publish(packet) => packet.write(&mut self.write)?,
            Packet::PubRel(packet) => packet.write(&mut self.write)?,
            Packet::PingReq => {
                let packet = PingReq;
                packet.write(&mut self.write)?
            }
            Packet::PingResp => {
                let packet = PingResp;
                packet.write(&mut self.write)?
            }
            Packet::Subscribe(packet) => packet.write(&mut self.write)?,
            Packet::SubAck(packet) => packet.write(&mut self.write)?,
            Packet::Unsubscribe(packet) => packet.write(&mut self.write)?,
            Packet::UnsubAck(packet) => packet.write(&mut self.write)?,
            Packet::Disconnect => {
                let packet = Disconnect;
                packet.write(&mut self.write)?
            }
            Packet::PubAck(packet) => packet.write(&mut self.write)?,
            Packet::PubRec(packet) => packet.write(&mut self.write)?,
            Packet::PubComp(packet) => packet.write(&mut self.write)?,
            _packet => unimplemented!(),
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

    pub async fn read_connect(&mut self) -> Result<Connect, io::Error> {
        let packet = self.read().await?;

        match packet {
            Packet::Connect(connect) => Ok(connect),
            packet => {
                let error = format!("Expecting connack. Received = {:?}", packet);
                Err(io::Error::new(io::ErrorKind::InvalidData, error))
            }
        }
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
