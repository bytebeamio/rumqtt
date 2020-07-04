use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use bytes::BytesMut;
use mqtt4bytes::*;

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
    max_packet_size: usize,
    /// Maximum readv count
    max_readv_count: usize,
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
            max_readv_count:10
        }
    }

    pub fn with_capacity(socket: impl N + 'static, read: usize, write: usize) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            pending: 0,
            read: BytesMut::with_capacity(read),
            write: BytesMut::with_capacity(write),
            max_packet_size: 1 * 1024,
            max_readv_count:10
        }
    }

    pub fn set_max_packet_size(&mut self, size: usize) {
        self.max_packet_size = size;
    }

    pub fn set_readv_count(&mut self, count: usize) {
        self.max_readv_count = count;
    }

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
                let read = self.fill().await?;
                total_read += read;
                if total_read >= self.pending {
                    self.pending = 0;
                    break;
                }
            }
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk
    pub async fn readb(&mut self) -> Result<Vec<Packet>, io::Error> {
        let mut out = Vec::with_capacity(self.max_readv_count);
        loop {
            match mqtt_read(&mut self.read, self.max_packet_size) {
                Ok(packet) => {
                    out.push(packet);
                    if out.len() >= self.max_readv_count { break }
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
                let read = self.fill().await?;
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
    async fn fill(&mut self) -> Result<usize, io::Error> {
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

    /// Write packet to network
    pub async fn write(&mut self, packet: Packet) -> Result<(), io::Error> {
        if let Err(e) = mqtt_write(packet, &mut self.write) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
        }

        self.flush().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        self.socket.write_all(&self.write[..]).await?;
        self.write.clear();
        Ok(())
    }

    pub async fn writeb(&mut self, packets: Vec<Packet>) -> Result<(), io::Error> {
        for packet in packets {
            if let Err(e) = mqtt_write(packet, &mut self.write) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
            }
        }

        self.flush().await?;
        Ok(())
    }
}

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
