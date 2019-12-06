use crate::{Notification, Request, network};
use derive_more::From;
use rumq_core::{self, Connect, Packet, Protocol, MqttRead, MqttWrite};
use futures_util::{select, FutureExt};
use futures_util::stream::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Elapsed};
use async_stream::stream;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use crate::state::{StateError, MqttState};
use crate::MqttOptions;

use std::io;
use std::time::Duration;

pub struct MqttEventLoop {
    state: MqttState,
    options: MqttOptions,
    queue_limit_tx: Sender<()>,
    queue_limit_rx: Receiver<()>,
    requests: Box<dyn Requests>,
    network: Box<dyn  Network>,
}

#[derive(From, Debug)]
pub enum EventLoopError {
    Io(io::Error),
    NoRequestStream,
    NoTcpStream,
    NoRequest,
    MqttState(StateError),
    StreamClosed,
    Timeout(Elapsed),
    Rumq(rumq_core::Error),
    Network(network::Error)
}


#[derive(Debug)]
pub struct MqttError {
    state: MqttState,
    error: EventLoopError
}

/// Eventloop implementation. The life of the event loop is dependent on request
/// stream provided by the user and tcp stream. The event loop will be disconnected
/// either during a disconnection at the broker or when the request stream ends.
/// TODO: The implementation of of user requests which are bounded streams (ends after
/// producing 'n' elements) is not very clear yet. Probably the stream should end when
/// all the state buffers are acked with a timeout
pub async fn eventloop(options: MqttOptions, requests: impl Requests + 'static) -> Result<impl MqttStream, EventLoopError> {
    let (queue_limit_tx, queue_limit_rx) = channel(1);
    let mut state = MqttState::new(options.clone());
    
    // make tcp and mqtt connections
    let mut network = network_connect(&options).await?;
    mqtt_connect(&options, &mut network, &mut state).await?;
    
    // make network and user requests generic for better unit testing capabilities
    let network = Box::new(network);
    let requests = Box::new(requests);
    // let requests = requests.throttle(self.options.throttle);

    let mut eventloop = MqttEventLoop {
        state,
        options,
        queue_limit_rx,
        queue_limit_tx,
        network,
        requests,
    };

    // a stream which polls user request stream, network stream and creates
    // a stream of notifications to the user
    let o = stream! {
        loop {
            let event = match eventloop.poll().await {
                Ok(o) => Ok(o),
                Err(error) => {
                    let state = eventloop.state.clone();
                    Err(MqttError { state, error: error.into() })
                }
            };

            let (notification, reply) = event?;

            // write the reply back to the network
            if let Some(p) = reply { 
                if let Err(error) = eventloop.network.mqtt_write(&p).await {
                    let state = eventloop.state.clone();
                    Err(MqttError { state, error: error.into() })?;
                }
            }
            // yield the notification to the user
             if let Some(n) = notification { yield Ok::<_, MqttError>(n) }
        }
    };

    // https://rust-lang.github.io/async-book/04_pinning/01_chapter.html
    let o = Box::pin(o);
    Ok(o)
}

async fn network_connect(options: &MqttOptions) -> Result<Box<dyn Network>, EventLoopError> {
    let network= time::timeout(Duration::from_secs(5), async {
        if options.ca.is_some() {
            let o = network::tls_connect(&options).await?;
            let o = Box::new(o);
            Ok::<Box<dyn Network>, EventLoopError>(o)
        } else {
            let o = network::tcp_connect(&options).await?;
            let o = Box::new(o);
            Ok::<Box<dyn Network>, EventLoopError>(o)
        }
    }).await??;

    Ok(network)
}


async fn mqtt_connect(options: &MqttOptions, mut network: impl Network, state: &mut MqttState) -> Result<(), EventLoopError> {
    let connect = connect_packet(options);
    
    // mqtt connection with timeout
    time::timeout(Duration::from_secs(5), async {
        network.mqtt_write(&connect).await?;
        state.handle_outgoing_connect()?;
        Ok::<_, EventLoopError>(())
    }).await??;

    // wait for 'timeout' time to validate connack
    time::timeout(Duration::from_secs(5), async {
        let packet = network.mqtt_read().await?;
        state.handle_incoming_connack(packet)?;
        Ok::<_, EventLoopError>(())
    }).await??;

    Ok(())
}


impl MqttEventLoop {
    async fn poll(&mut self) -> Result<(Option<Notification>, Option<Packet>), EventLoopError> {
        let network = &mut self.network;
        let requests = &mut self.requests;

        let network = time::timeout(self.options.keep_alive, async {
            let packet = network.mqtt_read().await?;
            Ok::<_, EventLoopError>(packet)
        });

        let request = time::timeout(self.options.keep_alive, async {            
            let request = requests.next().await;
            let request = request.ok_or(EventLoopError::NoRequest)?;
            Ok::<_, EventLoopError>(request)
        });

        select! {
            o = network.fuse() =>  {
                let (notification, request) = self.handle_packet(o).await?;
                Ok((notification, request))
            },
            o = request.fuse() => {
                let (notification, request) = self.handle_request(o).await?;
                Ok((notification, request))
            }
        }
    }

    async fn handle_packet(&mut self, packet: Result<Result<Packet, EventLoopError>, Elapsed>) -> Result<(Option<Notification>, Option<Packet>), EventLoopError> {
        let packet = match packet {
            Ok(packet) => packet,
            Err(_) => {
                self.state.handle_outgoing_ping()?;
                return Ok((None, Some(Packet::Pingreq)))
            }
        };

        let (notification, reply) = self.state.handle_incoming_mqtt_packet(packet?)?;
        if self.state.outgoing_pub.len() > self.options.inflight {
            self.queue_limit_tx.send(()).await;
        }

        Ok((notification, reply))
    }

    async fn handle_request(&mut self, request: Result<Result<Request, EventLoopError>, Elapsed>) -> Result<(Option<Notification>, Option<Packet>),  EventLoopError> {
        let request = match request {
            Ok(request) => request,
            Err(_) => {
                self.state.handle_outgoing_ping()?;
                return Ok((None, Some(Packet::Pingreq)))
            }
        };

        if self.state.outgoing_pub.len() > self.options.inflight {
            self.queue_limit_rx.recv().await;
        }

        // outgoing packet handle is only user for requests, not replys. this ensures
        // ping debug print show last request time, not reply time
        let request = self.state.handle_outgoing_mqtt_packet(request?.into());
        let request = Some(request);
        let notification = None;
        Ok((notification, request))
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
pub trait MqttStream: Stream<Item = Result<Notification, MqttError>> {}
impl<T: Stream<Item = Result<Notification, MqttError>>> MqttStream for T {}

trait Network: AsyncWrite + AsyncRead + Unpin + Send {}
impl<T> Network for T where T: AsyncWrite + AsyncRead + Unpin + Send {}

pub trait Requests: Stream<Item = Request> + Unpin {}
impl<T> Requests for T where T: Stream<Item = Request> + Unpin {}


#[cfg(test)]
mod test {
    use super::MqttEventLoop;
    use crate::state::MqttState;
    use crate::{MqttOptions, network};

    fn eventloop<I>() -> MqttEventLoop<I> {
        let options = MqttOptions::new("dummy", "test.mosquitto.org", 1883);
        let network = network::vconnect(options.clone());

        let eventloop = MqttEventLoop {
            state: MqttState::new(options.clone()),
            options,
            requests: None,
            network
        };

        eventloop
    }

    #[test]
    fn connection_should_timeout_on_time() {

    }

    #[test]
    fn connection_waits_for_connack_and_errors_after_timeout() {

    }

    #[test]
    fn disconnection_errors_honor_reconnection_options() {

    }

    #[test]
    fn throttled_requests_works_with_correct_delays_between_requests() {

    }

    #[test]
    fn request_future_triggers_pings_on_time() {

    }

    #[test]
    fn network_future_triggers_pings_on_time() {

    }

    #[test]
    fn requests_are_blocked_after_max_inflight_queue_size() {

    }

    #[test]
    fn requests_are_recovered_after_inflight_queue_size_falls_below_max() {

    }
}
