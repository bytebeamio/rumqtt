use bytes::BytesMut;
use mqtt4bytes::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::state::State;
use std::io::{self, ErrorKind};
use tokio::time::{self, Duration};

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
    max_readb_count: usize,
    keepalive: Duration,
}

impl Network {
    pub fn new(socket: impl N + 'static, max_incoming_size: usize) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            read: BytesMut::with_capacity(10 * 1024),
            max_incoming_size,
            max_readb_count: 10,
            keepalive: Duration::from_secs(0),
        }
    }

    pub fn set_keepalive(&mut self, keepalive: u16) {
        let keepalive = Duration::from_secs(keepalive as u64);
        self.keepalive = keepalive + keepalive.mul_f32(0.5);
    }

    /// Reads more than 'required' bytes to frame a packet into self.read buffer
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

    pub async fn readb(&mut self, state: &mut State) -> io::Result<()> {
        if self.keepalive.as_secs() > 0 {
            time::timeout(self.keepalive, async {
                self.collect(state).await?;
                Ok::<(), io::Error>(())
            })
            .await??;

            Ok::<(), io::Error>(())
        } else {
            self.collect(state).await?;
            Ok::<(), io::Error>(())
        }
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn collect(&mut self, state: &mut State) -> Result<(), io::Error> {
        let mut count = 0;
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
                    count += 1;

                    state.handle_network_data(packet);
                    if count >= self.max_readb_count {
                        return Ok(());
                    }
                }
                // Wait for more bytes until a frame can be created or return with existing
                Err(Error::InsufficientBytes(required)) => {
                    // If some packets are already framed, return those instead
                    // of blocking until max readb count
                    if count > 0 {
                        return Ok(());
                    }

                    self.read_bytes(required).await?;
                }
                Err(e) => return Err(io::Error::new(ErrorKind::InvalidData, e.to_string())),
            };
        }
    }

    pub async fn connack(&mut self, connack: ConnAck) -> Result<usize, io::Error> {
        let mut write = BytesMut::new();
        let len = match connack.write(&mut write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        self.socket.write_all(&write[..]).await?;
        Ok(len)
    }

    pub async fn flush(&mut self, write: &mut BytesMut) -> Result<(), io::Error> {
        if write.is_empty() {
            return Ok(());
        }

        self.socket.write_all(&write[..]).await?;
        write.clear();
        Ok(())
    }
}

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
