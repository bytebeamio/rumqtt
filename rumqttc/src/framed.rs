use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use bytes::BytesMut;
use mqtt4bytes::*;

use std::io;
use crate::Request;

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
    max_packet_size: usize,
    /// Maximum readv count
    _max_readv_count: usize,
}

impl Network {
    pub fn new(socket: impl N + 'static) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            pending: 0,
            read: BytesMut::with_capacity(10 * 1024),
            write: BytesMut::with_capacity(10 * 1024),
            max_packet_size: 1 * 1024,
            _max_readv_count:10
        }
    }

    pub fn _with_capacity(socket: impl N + 'static, read: usize, write: usize) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            pending: 0,
            read: BytesMut::with_capacity(read),
            write: BytesMut::with_capacity(write),
            max_packet_size: 1 * 1024,
            _max_readv_count:10
        }
    }

    // pub fn set_max_packet_size(&mut self, size: usize) {
    //     self.max_packet_size = size;
    // }

    // pub fn set_readv_count(&mut self, count: usize) {
    //     self.max_readv_count = count;
    // }

    pub async fn read(&mut self) -> Result<Packet, io::Error> {
        loop {
            match mqtt_read(&mut self.read, self.max_packet_size) {
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

    /// Read packets in bulk. This allow replies to be in bulk
    pub async fn _readb(&mut self) -> Result<Vec<Packet>, io::Error> {
        let mut out = Vec::with_capacity(self._max_readv_count);
        loop {
            match mqtt_read(&mut self.read, self.max_packet_size) {
                Ok(packet) => {
                    out.push(packet);
                    if out.len() >= self._max_readv_count { break }
                    continue;
                }
                Err(Error::InsufficientBytes(required)) => {
                    self.pending = required;
                    if out.len() > 0 { break }
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
                Err(io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer"))
            } else {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection broken by peer"))
            };
        }

        Ok(read)
    }

    fn write_fill(&mut self, request: Request) -> Result<usize, io::Error> {
        let ret = match request {
            Request::Publish(packet) => packet.write(&mut self.write),
            Request::PubRel(packet) => packet.write(&mut self.write),
            Request::PingReq => {
                let packet = PingReq;
                packet.write(&mut self.write)
            },
            Request::PingResp => {
                let packet = PingResp;
                packet.write(&mut self.write)
            }
            Request::Subscribe(packet) => packet.write(&mut self.write),
            Request::Unsubscribe(packet) => packet.write(&mut self.write),
            Request::Disconnect => {
                let packet = Disconnect;
                packet.write(&mut self.write)
            },
            Request::PubAck(packet) => packet.write(&mut self.write),
            Request::PubRec(packet) => packet.write(&mut self.write),
            Request::PubComp(packet) => packet.write(&mut self.write),
        };

        match ret {
            Ok(size) => Ok(size),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
        }
    }

    pub async fn connect(&mut self, connect: Connect) -> Result<usize, io::Error> {
        let len = match connect.write(&mut self.write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
        };

        self.flush().await?;
        Ok(len)
    }

    #[cfg(test)]
    pub async fn connack(&mut self, connack: ConnAck) -> Result<usize, io::Error> {
        let len = match connack.write(&mut self.write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
        };

        self.flush().await?;
        Ok(len)
    }

    /// Write packet to network
    pub async fn write(&mut self, request: Request) -> Result<usize, io::Error> {
        let len = self.write_fill(request)?;
        self.flush().await?;
        Ok(len)
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        self.socket.write_all(&self.write[..]).await?;
        self.write.clear();
        Ok(())
    }

    pub async fn _writeb(&mut self, requests: Vec<Request>) -> Result<(), io::Error> {
        for request in requests {
            self.write_fill(request)?;
        }

        self.flush().await?;
        Ok(())
    }
}

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
