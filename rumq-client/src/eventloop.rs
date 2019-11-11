use crate::{Notification, Request, network};
use crate::state::{StateError, MqttState};
use crate::MqttOptions;

use derive_more::From;

use rumq_core::{self, Connect, Packet, Protocol, MqttRead, MqttWrite};
use futures_core::Stream;
use futures_util::{select, StreamExt, FutureExt};

use std::{io, mem};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::timer;
use async_stream::stream;
use std::marker::PhantomData;
use crate::network::NetworkStream;


pub struct MqttEventLoop<I> {
    state: MqttState,
    options: MqttOptions,
    requests: Option<I>,
    network: NetworkStream
}

#[derive(From, Debug)]
pub enum EventLoopError {
    Io(io::Error),
    NoRequestStream,
    NoTcpStream,
    NoRequest,
    MqttState(StateError),
    StreamClosed,
    Timeout(timer::timeout::Elapsed),
    Rumq(rumq_core::Error),
    Network(network::Error)
}

pub async fn connect<I>(options: MqttOptions, timeout: Duration) -> Result<MqttEventLoop<I>, EventLoopError> {
    // new tcp connection with a timeout
    let mut stream = network::connect(options.clone(), timeout).await?;

    // new mqtt connection with timeout
    timer::Timeout::new(async {
        let connect = connect_packet(&options);
        stream.mqtt_write(&connect).await?;
        Ok::<_, EventLoopError>(())
    }, timeout).await??;

    let eventloop = MqttEventLoop {
        state: MqttState::new(options.clone()),
        options,
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
        let network = &mut self.network;
        // TODO: Find a way to lazy initialize this struct member to get away with unwrap
        //       during every poll
        let requests = self.requests.as_mut().unwrap();

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

fn connect_packet(mqttoptions: &MqttOptions) -> Packet {
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

    Packet::Connect(connect)
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



#[cfg(test)]
mod test {
    use super::MqttEventLoop;
    use crate::state::MqttState;

    fn eventloop() -> MqttEventLoop<I> {
        let eventloop = MqttEventLoop {
            state: MqttState::new(mqttoptions.clone()),
            options,
            requests: None,
            network: stream
        };

        eventloop
    }
}