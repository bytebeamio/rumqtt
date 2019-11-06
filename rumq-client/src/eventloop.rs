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
    requests: Option<I>,
    network: TcpStream,
}

#[derive(From, Debug)]
pub enum EventLoopError {
    Io(io::Error),
    NoRequestStream,
    NoRequest,
    MqttState(StateError),
    StreamClosed,
    Timeout(timer::timeout::Elapsed),
    Rumq(rumq_core::Error)
}

pub async fn connect<I>(mqttoptions: MqttOptions) -> Result<MqttEventLoop<I>, EventLoopError> {
    // new tcp connection with a timeout
    let mut stream = timer::Timeout::new( async {
        let s = TcpStream::connect("localhost:1883").await;
        s
    }, Duration::from_secs(10)).await??;

    // new mqtt connection with timeout
    let connect = Packet::Connect(connect_packet(&mqttoptions));
    timer::Timeout::new(async {
        stream.mqtt_write(&connect).await?;
        Ok::<_, EventLoopError>(())
    }, Duration::from_secs(10)).await??;

    let eventloop = MqttEventLoop {
        state: MqttState::new(mqttoptions.clone()),
        mqttoptions,
        requests: None,
        network: stream
    };

    Ok(eventloop)
}


/// Eventloop implementation. The life of the event loop is dependent on request
/// stream provided by the user and tcp stream. The event loop will be disconnected
/// either during a disconnection at the broker or when the request stream ends.
/// TODO: The implementation of of user requests which are bounded streams (ends after
/// producing 'n' elements) is not very clear yet. Probably the stream should end when
/// all the state buffers are acked with a timeout
impl<I: Stream<Item = Request> + Unpin> MqttEventLoop<I> {

    /// Build a stream object when polled by the user will start progress on incoming
    /// and outgoing data
    pub async fn build(&mut self, stream: I) -> Result<impl MqttStream + '_, EventLoopError> {
        self.requests = Some(stream);

        // a stream which polls user request stream, network stream and creates
        // a stream of notifications to the user
        let o = stream! {
            loop {
                let (notification, reply) = self.poll().await?;

                // write the reply back to the network
                if let Some(p) = reply {
                    self.network.mqtt_write(&p).await?;
                }

                // yield the notification to the user
                if let Some(n) = notification {
                    yield Ok(n)
                }
            }
        };

        // https://rust-lang.github.io/async-book/04_pinning/01_chapter.html
        let o = Box::pin(o);
        Ok(o)
    }

    async fn poll(&mut self) -> Result<(Option<Notification>, Option<Packet>), EventLoopError> {
        let mut network = &mut self.network;
        let mut requests = self.requests.as_mut().ok_or(EventLoopError::NoRequestStream)?;

        select! {
            packet = network.mqtt_read().fuse() =>  {
                let (notification, reply) = self.state.handle_incoming_mqtt_packet(packet?)?;
                Ok((notification, reply))
            },
            request = requests.next().fuse() => {
                let request = request.ok_or(EventLoopError::NoRequest)?;
                let request = self.state.handle_outgoing_mqtt_packet(request.into());
                let request = Some(request);
                let notification = None;
                Ok((notification, request))
            }
        }
    }
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

// impl trait alias hack: https://stackoverflow.com/questions/57937436/how-to-alias-an-impl-trait
pub trait MqttStream: Stream<Item = Result<Notification, EventLoopError>> {}
impl<T: Stream<Item = Result<Notification, EventLoopError>>> MqttStream for T {}