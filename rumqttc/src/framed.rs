use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::mqttbytes::{self, v4::*};
use crate::Incoming;
use std::io;

/// Errors while handling network
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Mqtt: {0}")]
    Mqtt(#[from] mqttbytes::Error),
}

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Socket for IO
    socket: Box<dyn N>,
    /// Buffered reads
    read: BytesMut,
    /// Maximum packet size
    max_incoming_size: usize,
    /// Maximum readv count
    pub(crate) max_readb_count: usize,
}

impl Network {
    pub fn new(socket: impl N + 'static, max_incoming_size: usize) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            read: BytesMut::with_capacity(10 * 1024),
            max_incoming_size,
            max_readb_count: 10,
        }
    }

    /// Reads more than 'required' bytes to frame a packet into self.read buffer
    async fn read_bytes(&mut self, required: usize) -> io::Result<usize> {
        let mut total_read = 0;
        loop {
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

            total_read += read;
            if total_read >= required {
                return Ok(total_read);
            }
        }
    }

    pub async fn read(&mut self, wait: bool) -> Result<Incoming, NetworkError> {
        loop {
            let required = match read(&mut self.read, self.max_incoming_size) {
                Ok(packet) => return Ok(packet),
                // Wait for more bytes until a frame can be created
                // NOTE: do so only for the first frame
                Err(mqttbytes::Error::InsufficientBytes(required)) if wait => required,
                Err(e) => return Err(NetworkError::Mqtt(e)),
            };

            // read more packets until a frame can be created. This function
            // blocks until a frame can be created. Use this in a select! branch
            self.read_bytes(required).await?;
        }
    }


    pub async fn connect(&mut self, connect: Connect) -> io::Result<usize> {
        let mut write = BytesMut::new();
        let len = match connect.write(&mut write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        self.socket.write_all(&write[..]).await?;
        Ok(len)
    }

    pub async fn flush(&mut self, write: &mut BytesMut) -> io::Result<()> {
        if write.is_empty() {
            return Ok(());
        }

        self.socket.write_all(&write[..]).await?;
        write.clear();
        Ok(())
    }
}

pub trait N: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
