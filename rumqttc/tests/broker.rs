use mqtt4bytes::*;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::select;
use tokio::stream::StreamExt;
use tokio::time;

use rumqttc::{Network, Request};

pub struct Broker {
    pub(crate) framed: Network,
    pub(crate) incoming: VecDeque<Packet>,
}

impl Broker {
    /// Create a new broker which accepts 1 mqtt connection
    pub async fn new(port: u16, send_connack: bool) -> Broker {
        let addr = format!("127.0.0.1:{}", port);
        let mut listener = TcpListener::bind(&addr).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let mut framed = Network::new(stream, 10 * 1024);
        let mut incoming = VecDeque::new();

        let packet = framed.readb(&mut incoming).await.unwrap();
        if let Packet::Connect(_) = packet {
            if send_connack {
                let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
                framed.connack(connack).await.unwrap();
            }
        } else {
            panic!("Expecting connect packet");
        }

        Broker { framed, incoming }
    }

    // Reads a publish packet from the stream with 2 second timeout
    pub async fn read_publish(&mut self) -> Option<Publish> {
        loop {
            let packet = time::timeout(Duration::from_secs(2), async {
                self.framed.readb(&mut self.incoming).await
            });

            match packet.await {
                Ok(Ok(Packet::Publish(publish))) => return Some(publish),
                Ok(Ok(Packet::PingReq)) => {
                    self.framed.write(Request::PingResp).await.unwrap();
                    continue;
                }
                Ok(Ok(packet)) => panic!("Expecting a publish. Received = {:?}", packet),
                Ok(Err(e)) => panic!("Error = {:?}", e),
                // timed out
                Err(_) => return None,
            }
        }
    }

    /// Reads next packet from the stream
    pub async fn read_packet(&mut self) -> Packet {
        let packet = time::timeout(Duration::from_secs(30), async {
            let p = self.framed.readb(&mut self.incoming).await;
            // println!("Broker read = {:?}", p);
            p.unwrap()
        });

        let packet = packet.await.unwrap();
        packet
    }

    pub async fn read_packet_and_respond(&mut self) -> Packet {
        let packet = time::timeout(Duration::from_secs(30), async {
            self.framed.readb(&mut self.incoming).await
        });
        let packet = packet.await.unwrap().unwrap();

        if let Packet::Publish(publish) = packet.clone() {
            if publish.pkid > 0 {
                let packet = PubAck::new(publish.pkid);
                self.framed.write(Request::PubAck(packet)).await.unwrap();
            }
        }

        packet
    }

    /// Reads next packet from the stream
    pub async fn blackhole(&mut self) -> Packet {
        loop {
            let _packet = self.framed.readb(&mut self.incoming).await.unwrap();
        }
    }

    /// Sends an acknowledgement
    pub async fn ack(&mut self, pkid: u16) {
        let packet = Request::PubAck(PubAck::new(pkid));
        self.framed.write(packet).await.unwrap();
    }

    /// Send a bunch of publishes and ping response
    pub async fn start_publishes(&mut self, count: u8, qos: QoS, delay: Duration) {
        let mut interval = time::interval(delay);
        for i in 0..count {
            select! {
                _ = interval.next() => {
                    let topic = "hello/world".to_owned();
                    let payload = vec![1, 2, 3, i];
                    let publish = Publish::new(topic, qos, payload);
                    let packet = Request::Publish(publish);
                    self.framed.write(packet).await.unwrap();
                }
                packet = self.framed.readb(&mut self.incoming) => {
                    if let Packet::PingReq = packet.unwrap() {
                        self.framed.write(Request::PingResp).await.unwrap();
                    }
                }
            }
        }
    }
}
