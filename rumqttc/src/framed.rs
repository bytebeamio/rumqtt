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
    max_packet_size: usize
}

impl Network {
    pub fn new(socket: impl N + 'static) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            pending: 0,
            read: BytesMut::with_capacity(10 * 1024),
            write: BytesMut::with_capacity(10 * 1024),
            max_packet_size: 1 * 1024
        }
    }

    pub async fn read(&mut self) -> Result<Packet, io::Error> {
        loop {
            match mqtt_read(&mut self.read, self.max_packet_size) {
                Ok(packet) => return Ok(packet),
                Err(Error::InsufficientBytes(required)) => {
                    self.pending = required;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            }

            // read more packets until a frame can be created. This functions blocks until a frame
            // can be created. Use this in a select! branch
            let mut total_read = 0;
            loop {
                let read = self.socket.read_buf(&mut self.read).await?;
                if 0 == read {
                    return if self.read.is_empty() {
                        Err(io::Error::new(io::ErrorKind::ConnectionReset, "connection reset by peer"))
                    } else {
                        Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection broken by peer"))
                    };
                }

                total_read += read;
                if total_read >= self.pending {
                    self.pending = 0;
                    break;
                }
            }
        }
    }

    pub async fn readv(&mut self) -> Result<Vec<Packet>, io::Error> {
        todo!()
    }

    pub async fn write(&mut self, packet: Packet) -> Result<(), io::Error> {
        if let Err(e) = mqtt_write(packet, &mut self.write) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
        }

        self.socket.write_all(&self.write[..]).await?;
        self.write.clear();
        Ok(())
    }

    pub async fn writev(&mut self, packets: Vec<Packet>) -> Result<(), io::Error> {
        todo!()
    }
}


pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
