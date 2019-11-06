use crate::Notification;
use crate::state::{StateError, MqttState};
use crate::MqttOptions;
use crate::Request;

use derive_more::From;

use rumq_core::{self, Connect, Packet, Protocol, MqttRead, MqttWrite};
use futures_core::Stream;
use futures_util::{select, FusedStream, StreamExt, SinkExt, FutureExt, pin_mut};
use futures_util::stream::{SplitStream, SplitSink, Fuse};

use std::io;
use std::time::Duration;

use tokio::net::{self, TcpStream};
use tokio::timer;
use async_stream::stream;
use tokio_io::split::ReadHalf;
use tokio::prelude::AsyncRead;
use tokio::io::AsyncReadExt;


pub struct MqttEventLoop<I> {
    state: MqttState,
    mqttoptions: MqttOptions,
    requests: I,
    network: TcpStream,
}

#[derive(From, Debug)]
pub enum EventLoopError {
    Io(io::Error),
    NoRequest,
    MqttState(StateError),
    StreamClosed,
    Timeout(timer::timeout::Elapsed),
    Rumq(rumq_core::Error)
}

pub async fn connect<I: Stream<Item = Request>>(mqttoptions: MqttOptions, requests: I) -> Result<MqttEventLoop<I>, EventLoopError> {
    let stream = connect_timeout().await?;
 
    let eventloop = MqttEventLoop {
        state: MqttState::new(mqttoptions.clone()),
        mqttoptions,
        requests,
        network: stream
    };

    Ok(eventloop)
}

async fn connect_timeout() -> Result<TcpStream, io::Error> {
    let connection = timer::Timeout::new( async {
        let s = TcpStream::connect("localhost:1883").await;
        s
    }, Duration::from_secs(10)).await??;


    Ok(connection)
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

impl<I: Stream<Item = Request> + Unpin> MqttEventLoop<I> {
    async fn start(&mut self) -> Result<(), EventLoopError> {

        return Ok::<_, EventLoopError>(())
    }

    async fn poll(&mut self) -> Result<(Option<Notification>, Option<Packet>), EventLoopError> {
        select! {
            packet = self.network.mqtt_read().fuse() =>  {
                let (notification, reply) = self.state.handle_incoming_mqtt_packet(packet?)?;
                Ok((notification, reply))
            },
            request = self.requests.next().fuse() => {
                let request = request.ok_or(EventLoopError::NoRequest)?;
                let request = self.state.handle_outgoing_mqtt_packet(request.into());
                let request = Some(request);
                let notification = None;
                Ok((notification, request))
            }
        }
    }
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

