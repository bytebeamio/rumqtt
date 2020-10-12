use mqtt4bytes::*;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::select;
use tokio::{task, time};

use async_channel::{bounded, Receiver, Sender};
use rumqttc::{Event, Network, Request};

pub struct Broker {
    pub(crate) framed: Network,
    pub(crate) incoming: VecDeque<Packet>,
    outgoing_tx: Sender<Request>,
    outgoing_rx: Receiver<Request>,
}

impl Broker {
    /// Create a new broker which accepts 1 mqtt connection
    pub async fn new(port: u16, connack: u8) -> Broker {
        let addr = format!("127.0.0.1:{}", port);
        let mut listener = TcpListener::bind(&addr).await.unwrap();

        let (stream, _) = listener.accept().await.unwrap();
        let mut framed = Network::new(stream, 10 * 1024);
        let mut incoming = VecDeque::new();
        let (outgoing_tx, outgoing_rx) = bounded(10);

        match framed.readb(&mut incoming).await.unwrap() {
            Packet::Connect(_) => {
                let connack = match connack {
                    0 => ConnAck::new(ConnectReturnCode::Accepted, false),
                    1 => ConnAck::new(ConnectReturnCode::BadUsernamePassword, false),
                    _ => {
                        return Broker {
                            framed,
                            incoming,
                            outgoing_tx,
                            outgoing_rx,
                        }
                    }
                };

                framed.connack(connack).await.unwrap();
            }
            _ => {
                panic!("Expecting connect packet");
            }
        }

        Broker {
            framed,
            incoming,
            outgoing_tx,
            outgoing_rx,
        }
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

    pub async fn _read_packet_and_respond(&mut self) -> Packet {
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

    /// Sends an acknowledgement
    pub async fn pingresp(&mut self) {
        let packet = Request::PingResp;
        self.framed.write(packet).await.unwrap();
    }

    pub async fn spawn_publishes(&mut self, count: u8, qos: QoS, delay: u64) {
        let tx = self.outgoing_tx.clone();

        task::spawn(async move {
            for i in 1..=count {
                let topic = "hello/world".to_owned();
                let payload = vec![1, 2, 3, i];
                let mut publish = Publish::new(topic, qos, payload);

                if qos as u8 > 0 {
                    publish.pkid = i as u16;
                }

                let packet = Request::Publish(publish);
                tx.send(packet).await.unwrap();
                time::delay_for(Duration::from_secs(delay)).await;
            }
        });
    }

    /// Selects between outgoing and incoming packets
    pub async fn tick(&mut self) -> Event {
        select! {
            request = self.outgoing_rx.recv() => {
                let request = request.unwrap();
                let outgoing = self.framed.write(request).await.unwrap();
                Event::Outgoing(outgoing)
            }
            packet = self.framed.readb(&mut self.incoming) => {
                let incoming = packet.unwrap();
                Event::Incoming(incoming)
            }
        }
    }
}
