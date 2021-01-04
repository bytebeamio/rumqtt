use bytes::BytesMut;
use mqtt5bytes::{mqtt_read, Connect, Error, Packet, Publish, QoS, Subscribe};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main(worker_threads = 2)]
async fn main() {
    let mut client = Client::new().await;

    // send connect
    let connect = Packet::Connect(Connect::new("mqtt5test"));
    client.write(connect).await.unwrap();

    // read connack
    let packet = client.read().await.unwrap();
    println!("{:?}", packet);

    // send subscribe
    let mut subscribe = Subscribe::new("hello/world", QoS::AtLeastOnce);
    subscribe.pkid = 1;
    client.write(Packet::Subscribe(subscribe)).await.unwrap();

    for i in 2..=11 {
        let mut publish = Publish::new("hello/world", QoS::AtLeastOnce, "hello mqtt 5");
        publish.set_pkid(i);
        client.write(Packet::Publish(publish)).await.unwrap();
    }

    loop {
        let packet = client.read().await.unwrap();
        println!("{:?}", packet);
    }
}

pub struct Client {
    socket: TcpStream,
    /// Pending bytes required to create next frame
    pending: usize,
    /// Buffered reads
    read: BytesMut,
    /// Buffered writes
    write: BytesMut,
}

impl Client {
    pub async fn new() -> Client {
        let socket = TcpStream::connect(("localhost", 1883)).await.unwrap();
        Client {
            socket,
            pending: 0,
            read: BytesMut::new(),
            write: BytesMut::new(),
        }
    }

    pub async fn read(&mut self) -> Result<Packet, io::Error> {
        loop {
            match mqtt_read(&mut self.read, 1024) {
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

    #[inline]
    async fn write(&mut self, packet: Packet) -> Result<usize, Error> {
        let size = match packet {
            Packet::Connect(packet) => packet.write(&mut self.write)?,
            Packet::Publish(packet) => packet.write(&mut self.write)?,
            Packet::PubAck(packet) => packet.write(&mut self.write)?,
            Packet::PubRec(packet) => packet.write(&mut self.write)?,
            Packet::PubComp(packet) => packet.write(&mut self.write)?,
            Packet::Subscribe(packet) => packet.write(&mut self.write)?,
            Packet::SubAck(packet) => packet.write(&mut self.write)?,
            _ => todo!(),
        };

        self.socket.write_all(&self.write[..]).await.unwrap();
        self.write.clear();
        Ok(size)
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
}
