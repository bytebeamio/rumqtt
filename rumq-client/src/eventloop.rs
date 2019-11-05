use crate::Notification;
use crate::state::Reply;
use crate::state::{StateError, MqttState};
use crate::MqttOptions;
use crate::Request;

use derive_more::From;

use rumq_core::{Connect, Packet, Protocol};
use futures_core::Stream;
use futures_util::{select, FusedStream, StreamExt, SinkExt};
use futures_util::stream::{SplitStream, SplitSink, Fuse};

use std::io;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::timer;
use async_stream::stream;


pub struct MqttEventLoop<I: Stream<Item = Request> + FusedStream + Unpin> {
    state: MqttState,
    mqttoptions: MqttOptions,
    requests: I,
    network_tx: SplitSink<Framed<TcpStream, MqttCodec>, Packet>,
    network_rx: Fuse<SplitStream<Framed<TcpStream, MqttCodec>>>
}

#[derive(From, Debug)]
pub enum EventLoopError {
    Io(io::Error),
    MqttState(StateError),
    StreamClosed,
    Timeout(timer::timeout::Elapsed),
}

pub async fn connect<I: Stream<Item = Request> + FusedStream + Unpin>(mqttoptions: MqttOptions, requests: I) -> Result<MqttEventLoop<I>, EventLoopError> { 
        let stream = connect_timeout().await?;
        let (network_tx, network_rx) = stream.split();

        let network_rx = network_rx.fuse();
        let eventloop = MqttEventLoop {
            state: MqttState::new(mqttoptions.clone()),
            mqttoptions,
            requests,
            network_tx,
            network_rx
        };

        Ok(eventloop)
    }

impl<I: Stream<Item = Request> + FusedStream + Unpin> MqttEventLoop<I> {
    async fn select(&mut self) -> Result<Option<Notification>, EventLoopError> {
        select! {
            packet = self.network_rx.next() =>  {
                let packet = if let Some(p) = packet {
                    p?
                } else {
                    return Ok(None)
                };

                let (notification, reply) = self.state.handle_incoming_mqtt_packet(packet)?;
                
                if let Some(reply) = reply {
                    self.network_tx.send(reply.into()).await?;
                }

                Ok(notification)
            },
            request = self.requests.next() => {
                let request = request.unwrap();
                let request = self.state.handle_outgoing_mqtt_packet(request.into());
                self.network_tx.send(request.into()).await?;
                Ok(None)
            }
        }
    }

    pub async fn eventloop(&mut self) -> Result<impl Stream<Item = Result<Notification, EventLoopError>> + '_, EventLoopError> {
        let connect = Packet::Connect(connect_packet(&self.mqttoptions));

        // TODO: timeout doesn't work on python3 -m http.server
        timer::Timeout::new(async {
                self.network_tx.send(connect).await?;
                Ok::<_, io::Error>(())
            }, Duration::from_secs(10),
        ).await??;

        let o = stream!{
            loop {
                let o = if let Some(notification) = self.select().await? {
                    notification
                } else {
                    continue
                };

                yield Ok(o)
            }
        };

        // https://rust-lang.github.io/async-book/04_pinning/01_chapter.html
        let o = Box::pin(o);
        Ok(o)
    }
}

async fn connect_timeout() -> Result<Framed<TcpStream, MqttCodec>, io::Error> {
    let connection = timer::Timeout::new( async {
        TcpStream::connect("localhost:1883").await
    }, Duration::from_secs(10)).await??;

    let framed = Framed::new(connection, MqttCodec);

    Ok(framed)
}

fn connect_packet(mqttoptions: &MqttOptions) -> Connect {
    let (username, password) = if let Some((u, p)) = mqttoptions.credentials() {
        (Some(u), Some(p))
    } else {
        (None, None)
    };
    let connect = Connect {
        protocol: Protocol::MQTT(4),
        keep_alive: mqttoptions.keep_alive().as_secs() as u16,
        client_id: mqttoptions.client_id(),
        clean_session: mqttoptions.clean_session(),
        last_will: None,
        username,
        password,
    };
    connect
}

impl From<Request> for Packet {
    fn from(item: Request) -> Self {
        match item {
            Request::Publish(publish) => Packet::Publish(publish),
            Request::Disconnect => Packet::Disconnect,
            Request::Subscribe(subscribe) => Packet::Subscribe(subscribe),
            Request::Unsubscribe(unsubscribe) => Packet::Unsubscribe(unsubscribe),
            _ => unimplemented!(),
        }
    }
}

impl From<Reply> for Packet {
    fn from(item: Reply) -> Self {
        match item {
            Reply::PubAck(p) => Packet::Puback(p),
            _ => unimplemented!(),
        }
    }
}