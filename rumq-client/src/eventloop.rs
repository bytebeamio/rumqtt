use crate::{Notification, Request, network};
use derive_more::From;
use rumq_core::{self, Connect, Packet, Protocol, MqttRead, MqttWrite};
use futures_util::{select, FutureExt};
use futures_util::stream::{Stream, StreamExt};
use futures_util::pin_mut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Elapsed};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use pin_project::pin_project;
use crate::state::{StateError, MqttState};
use crate::MqttOptions;

use std::io;
use std::time::Duration;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[pin_project]
pub struct MqttEventLoop {
    pub state: MqttState,
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

/// Eventloop which drives the client. Connects to the server and returs a stream to 
/// be polled to handle incoming network packets and outgoing user requests. The
/// implementation right now does tcp and mqtt connects before returning the eventloop.
/// This might interfere with commands that needs to be catered on priority even during
/// connections (e.g shutdown can't wait for a timeout of 5 seconds while connecting)
/// TODO: Handle shutdown during connection
/// TODO: The implementation of of user requests which are bounded streams (ends after
/// producing 'n' elements) is not very clear yet. Probably the stream should end when
/// all the state buffers are acked with a timeout
pub async fn eventloop(options: MqttOptions, requests: impl Requests + 'static) -> Result<MqttEventLoop, EventLoopError> {
    let (queue_limit_tx, queue_limit_rx) = channel(1);
    let mut state = MqttState::new(options.clone());
    
    // make tcp and mqtt connections
    let mut network = network_connect(&options).await?;
    mqtt_connect(&options, &mut network, &mut state).await?;
    
    // make network and user requests generic for better unit testing capabilities
    let network = Box::new(network);
    let requests = Box::new(requests);
    // let requests = requests.throttle(self.options.throttle);

    let eventloop = MqttEventLoop {
        state,
        options,
        queue_limit_rx,
        queue_limit_tx,
        network,
        requests,
    };

    Ok(eventloop)
}


impl Stream for MqttEventLoop {
    type Item = Notification;


    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Notification>> {
        let mqtt = self.get_mut().mqtt();
        pin_mut!(mqtt);

        match mqtt.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(v) => match v {
               Ok(Some(v)) => Poll::Ready(Some(v)),
               Ok(None) => Poll::Pending,
               Err(e) => {
                   error!("Mqtt eventloop error = {:?}", e);
                   Poll::Ready(None)
               }
            }
        }
    }
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
    async fn mqtt(&mut self) -> Result<Option<Notification>, EventLoopError> {
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

        let (notification, reply) = select! {
            o = network.fuse() =>  {
                let (notification, request) = self.handle_packet(o).await?;
                (notification, request)
            },
            o = request.fuse() => {
                let (notification, request) = self.handle_request(o).await?;
                (notification, request)
            }
        };

        if let Some(packet) = reply {
            self.network.mqtt_write(&packet).await?;
        }


        Ok(notification)
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

trait Network: AsyncWrite + AsyncRead + Unpin + Send {}
impl<T> Network for T where T: AsyncWrite + AsyncRead + Unpin + Send {}

pub trait Requests: Stream<Item = Request> + Unpin {}
impl<T> Requests for T where T: Stream<Item = Request> + Unpin {}


#[cfg(test)]
mod test {
    use super::MqttEventLoop;
    use crate::state::MqttState;
    use crate::{MqttOptions, network};

    // fn eventloop<I>() -> MqttEventLoop<I> {
    //     let options = MqttOptions::new("dummy", "test.mosquitto.org", 1883);
    //     let mut state = MqttState::new(options.clone());
    //     let (queue_limit_tx, queue_limit_rx) = channel(1);
    //     
    //     // make tcp and mqtt connections
    //     let mut network = network_connect(&options).await?;
    //     let mut network = 
    //     
    //     // make network and user requests generic for better unit testing capabilities
    //     let network = Box::new(network);
    //     let requests = Box::new(requests);
    //     // let requests = requests.throttle(self.options.throttle);

    //     let eventloop = MqttEventLoop {
    //         state,
    //         options,
    //         queue_limit_rx,
    //         queue_limit_tx,
    //         network,
    //         requests,
    //     };

    //     Ok(eventloop)
    // }

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
