use std::io;

use bytes::{Bytes, BytesMut};
use log::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    protocol::{self, v4, v5, Connect, Packet},
    Error,
};

pub(crate) struct Network {
    stream: TcpStream,
    buf: BytesMut,
    protocol_level: u8,
}

impl Network {
    pub(crate) async fn read_connect(stream: TcpStream) -> Result<(Self, Connect), Error> {
        let mut network = Self {
            stream,
            buf: BytesMut::with_capacity(4096),
            protocol_level: 0,
        };
        debug!("network: reading from stream");
        network.stream.read_buf(&mut network.buf).await?;
        let connect_packet = loop {
            match protocol::read_first_connect(&mut network.buf, 4096) {
                Err(protocol::Error::InsufficientBytes(count)) => {
                    network.read_atleast(count).await?
                }
                res => break res?,
            }
        };
        debug!("network: read connect");
        match &connect_packet {
            Connect::V4(_) => {
                network.protocol_level = 4;
                let mut payload = BytesMut::with_capacity(10);
                v4::connack::write(v4::connack::ConnectReturnCode::Success, false, &mut payload)?;
                network.send_data(&payload.split().freeze()).await?;
            }
            Connect::V5(_) => {
                network.protocol_level = 5;
                let mut payload = BytesMut::with_capacity(10);
                v5::connack::write(
                    v5::connack::ConnectReturnCode::Success,
                    false,
                    None,
                    &mut payload,
                )?;
                network.send_data(&payload.split().freeze()).await?;
            }
        }
        debug!("network: sent connack");
        Ok((network, connect_packet))
    }

    async fn read_atleast(&mut self, count: usize) -> io::Result<()> {
        let mut len = 0;
        while len < count {
            len += self.stream.read_buf(&mut self.buf).await?;
        }
        debug!("network: read {} bytes", len);

        Ok(())
    }

    pub(crate) async fn poll(&mut self) -> Result<protocol::Packet, Error> {
        loop {
            match self.protocol_level {
                4 => match v4::read_mut(&mut self.buf, 4096) {
                    Err(protocol::Error::InsufficientBytes(count)) => {
                        self.read_atleast(count).await?;
                        continue;
                    }
                    res => return Ok(Packet::V4(res?)),
                },
                5 => match v5::read_mut(&mut self.buf, 4096) {
                    Err(protocol::Error::InsufficientBytes(count)) => {
                        self.read_atleast(count).await?;
                        continue;
                    }
                    res => return Ok(Packet::V5(res?)),
                },
                // SAFETY: we don't allow changing protocol_level
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
    }

    pub(crate) async fn send_data(&mut self, data: &Bytes) -> Result<(), Error> {
        debug!(
            "network: sent {} bytes",
            self.stream.write(data.as_ref()).await?
        );
        Ok(())
    }
}
