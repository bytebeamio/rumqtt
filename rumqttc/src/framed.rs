use bytes::BytesMut;
use mqtt4bytes::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Incoming, Outgoing, Request};
use std::collections::VecDeque;
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

    pub fn with_capacity(
        socket: impl N + 'static,
        read: usize,
        write: usize,
        max_incoming_size: usize,
    ) -> Network {
        let socket = Box::new(socket) as Box<dyn N>;
        Network {
            socket,
            pending: 0,
            read: BytesMut::with_capacity(read),
            write: BytesMut::with_capacity(write),
            max_incoming_size,
            max_readb_count: 10,
        }
    }

    pub fn set_readv_count(&mut self, count: usize) {
        self.max_readb_count = count;
    }

    pub async fn read(&mut self) -> Result<Incoming, io::Error> {
        loop {
            match mqtt_read(&mut self.read, self.max_incoming_size) {
                Ok(packet) => return Ok(packet),
                Err(Error::InsufficientBytes(required)) => self.pending = required,
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
    }

    /// Read packets in bulk. This allow replies to be in bulk. This method is used
    /// after the connection is established to read a bunch of incoming packets
    pub async fn readb(
        &mut self,
        incoming: &mut VecDeque<Incoming>,
    ) -> Result<Incoming, io::Error> {
        // Ensures that duplicate readb call without emptying `incoming` buffer
        // doesn't hit network causing indefinite block when there is no network
        // activity. But this triggers duplicate state handling in case this
        // function is called before emptying buffered `incoming` causing errors.
        // This block is left here as fixing errors is easier than fixing blocks.
        // Note: This might be removed in future
        // Note: `incoming` is passed by the caller instead of embedding in `self`
        // to prevent bringing state into `self` (network)
        if let Some(incoming) = incoming.pop_front() {
            return Ok(incoming);
        }

        loop {
            match mqtt_read(&mut self.read, self.max_incoming_size) {
                Ok(packet) => {
                    incoming.push_back(packet);
                    if incoming.len() >= self.max_readb_count {
                        break;
                    }

                    continue;
                }
                Err(Error::InsufficientBytes(required)) => {
                    self.pending = required;
                    // Not `max_readb_count` but we have atleast one packet.
                    if !incoming.is_empty() {
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

        Ok(incoming.pop_front().unwrap())
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
    fn write_fill(&mut self, request: Request) -> Result<usize, Error> {
        // TODO Implement max_outgoing_packet_size using write size
        let size = match request {
            Request::Publish(packet) => packet.write(&mut self.write)?,
            Request::PublishRaw(packet) => packet.write(&mut self.write)?,
            Request::PubRel(packet) => packet.write(&mut self.write)?,
            Request::PingReq => {
                let packet = PingReq;
                packet.write(&mut self.write)?
            }
            Request::PingResp => {
                let packet = PingResp;
                packet.write(&mut self.write)?
            }
            Request::Subscribe(packet) => packet.write(&mut self.write)?,
            Request::SubAck(packet) => packet.write(&mut self.write)?,
            Request::Unsubscribe(packet) => packet.write(&mut self.write)?,
            Request::UnsubAck(packet) => packet.write(&mut self.write)?,
            Request::Disconnect => {
                let packet = Disconnect;
                packet.write(&mut self.write)?
            }
            Request::PubAck(packet) => packet.write(&mut self.write)?,
            Request::PubRec(packet) => packet.write(&mut self.write)?,
            Request::PubComp(packet) => packet.write(&mut self.write)?,
        };

        Ok(size)
    }

    pub async fn connect(&mut self, connect: Connect) -> Result<usize, io::Error> {
        let len = match connect.write(&mut self.write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        self.flush().await?;
        Ok(len)
    }

    pub async fn connack(&mut self, connack: ConnAck) -> Result<usize, io::Error> {
        let len = match connack.write(&mut self.write) {
            Ok(size) => size,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        self.flush().await?;
        Ok(len)
    }

    pub fn fill(&mut self, request: Request) -> Result<Outgoing, io::Error> {
        let outgoing = outgoing(&request);
        if let Err(e) = self.write_fill(request) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
        };

        Ok(outgoing)
    }

    pub fn fill2(&mut self, request: Request) -> Result<(), io::Error> {
        if let Err(e) = self.write_fill(request) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
        };

        Ok(())
    }

    /// Write packet to network
    pub async fn write(&mut self, request: Request) -> Result<Outgoing, io::Error> {
        let outgoing = self.fill(request)?;
        self.flush().await?;
        Ok(outgoing)
    }

    pub async fn flush(&mut self) -> Result<(), io::Error> {
        if self.write.is_empty() {
            return Ok(());
        }

        self.socket.write_all(&self.write[..]).await?;
        self.write.clear();
        Ok(())
    }

    pub async fn writeb(&mut self, requests: Vec<Request>) -> Result<(), io::Error> {
        for request in requests {
            self.fill(request)?;
        }

        self.flush().await?;
        Ok(())
    }
}

fn outgoing(packet: &Request) -> Outgoing {
    match packet {
        Request::Publish(publish) => Outgoing::Publish(publish.pkid),
        Request::PublishRaw(publish) => Outgoing::Publish(publish.pkid),
        Request::PubAck(puback) => Outgoing::PubAck(puback.pkid),
        Request::PubRec(pubrec) => Outgoing::PubRec(pubrec.pkid),
        Request::PubRel(pubrel) => Outgoing::PubRel(pubrel.pkid),
        Request::PubComp(pubcomp) => Outgoing::PubComp(pubcomp.pkid),
        Request::Subscribe(subscribe) => Outgoing::Subscribe(subscribe.pkid),
        Request::Unsubscribe(unsubscribe) => Outgoing::Unsubscribe(unsubscribe.pkid),
        Request::PingReq => Outgoing::PingReq,
        Request::PingResp => Outgoing::PingResp,
        Request::Disconnect => Outgoing::Disconnect,
        packet => panic!("Invalid outgoing packet = {:?}", packet),
    }
}

pub trait N: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
impl<T> N for T where T: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
