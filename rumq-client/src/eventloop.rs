use crate::{Notification, Request, network};
use derive_more::From;
use rumq_core::{self, Packet, MqttRead, MqttWrite};
use futures_util::{select, FutureExt};
use futures_util::stream::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Instant, Elapsed};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use async_stream::stream;
use pin_project::pin_project;
use crate::state::{StateError, MqttState};
use crate::MqttOptions;

use std::io;
use std::time::Duration;

#[pin_project]
pub struct MqttEventLoop {
    pub state: MqttState,
    options: MqttOptions,
    queue_limit_tx: Sender<()>,
    queue_limit_rx: Receiver<()>,
    requests: Box<dyn Requests>,
    network: Box<dyn  Network>,
    throttle_flag: bool,
    throttle: Instant
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

    let eventloop = MqttEventLoop {
        state,
        options,
        queue_limit_rx,
        queue_limit_tx,
        network,
        requests,
        throttle_flag: false,
        throttle: Instant::now()
    };

    Ok(eventloop)
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
    pub fn mqtt(&mut self) -> impl Stream<Item = Notification> + '_ {
        let o = stream! {
            loop {
                let (notification, reply) = match self.read_network_and_requests().await {
                    Ok(o) => o,
                    Err(e) => {
                        error!("Eventloop error = {:?}", e);
                        break
                    }
                };

                // write the reply back to the network
                if let Some(p) = reply {
                    match self.network.mqtt_write(&p).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Network write error = {:?}", e);
                            break
                        }
                    }
                }

                // yield the notification to the user
                if let Some(n) = notification { 
                    yield n 
                }
            }
        };
        
        // https://rust-lang.github.io/async-book/04_pinning/01_chapter.html
        Box::pin(o)
    }

    /// Reads user requests stream and network stream concurrently and returns notification
    /// which should be sent to the user and packet which should be written to network
    async fn read_network_and_requests(&mut self) -> Result<(Option<Notification>, Option<Packet>), EventLoopError> {
        let network = &mut self.network;
        let requests = &mut self.requests;
        let throttle = &mut self.throttle;
        let delay = self.options.throttle;
        let throttle_flag = &mut self.throttle_flag;

        let network = time::timeout(self.options.keep_alive, async {
            let packet = network.mqtt_read().await?;
            Ok::<_, EventLoopError>(packet)
        });

        let request = time::timeout(self.options.keep_alive, async {            
            // apply throttling to requests. throttling is only applied
            // after the last element is received to cater early returns
            // due to timeoutss. after the delay, if timeout has happened 
            // just before a request, next poll shouldn't cause same delay
            //
            // delay_until is used instead of delay_for incase timeout
            // happens just before delay ends, next poll's delay shouldn't
            // wait for `throttle` time. instead it should wait remaining time
            if *throttle_flag && delay.is_some() {
                time::delay_until(*throttle).await;
                *throttle_flag = false;
            }

            let request = requests.next().await;
            *throttle_flag = true;

            // Add delay for the next request
            if let Some(delay) = delay {
                *throttle = Instant::now() + delay;
            }

            let request = request.ok_or(EventLoopError::NoRequest)?;
            Ok::<_, EventLoopError>(request)
        });

        let (notification, outpacket) = select! {
            o = network.fuse() => self.handle_packet(o).await?,
            o = request.fuse() => self.handle_request(o).await?
        };



        Ok((notification, outpacket))
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
            let _ = self.queue_limit_tx.send(()).await;
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
    let mut connect = rumq_core::connect(mqttoptions.client_id());

    connect
        .set_keep_alive(mqttoptions.keep_alive().as_secs() as u16)
        .set_clean_session(mqttoptions.clean_session());

    if let Some((username, password)) = mqttoptions.credentials() {
        connect.set_username(username).set_password(password);
    }

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
    use rumq_core::*;
    use tokio::sync::mpsc::{channel, Sender, Receiver};
    use tokio::io::{self, AsyncRead, AsyncWrite};
    use tokio::time;
    use tokio::task;
    use futures_util::pin_mut;
    use futures_util::stream::StreamExt;
    use std::time::{Instant, Duration};
    use std::pin::Pin;
    use std::task::{Poll, Context};
    use std::future::Future;
    use std::sync::Arc;
    use std::thread;
    use super::MqttEventLoop;
    use crate::state::MqttState;
    use crate::{Request, MqttOptions};

    fn eventloop(requests: Receiver<Request>) -> (MqttEventLoop, Sender<Vec<u8>>, Receiver<Vec<u8>>) {
        let options = MqttOptions::new("dummy", "test.mosquitto.org", 1883);
        let state = MqttState::new(options.clone());
        let (network, server_tx, server_rx) = IO::new();
        let network = Box::new(network);
        let requests = Box::new(requests);
        let (queue_limit_tx, queue_limit_rx) = channel(1);


        let eventloop = MqttEventLoop {
            state,
            options,
            queue_limit_rx,
            queue_limit_tx,
            network,
            requests,
            throttle_flag: false,
            throttle: time::Instant::now()
        };

        (eventloop, server_tx, server_rx)
    }


    fn publish_request(i: u8) -> Request {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = publish(topic, payload);
        Request::Publish(publish)
    }

    #[tokio::main(basic_scheduler)]
    async fn requests(mut requests_tx: Sender<Request>) {
        task::spawn(async move {
            for i in 0..10 {
                requests_tx.send(publish_request(i)).await.unwrap();
            }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn connection_should_timeout_on_time() {
        let (requests_tx, requests_rx) = channel(5);
        let (mut eventloop, tx, rx) = eventloop(requests_rx);
        let start = Instant::now(); 
        let o = super::mqtt_connect(&eventloop.options, &mut eventloop.network, &mut eventloop.state).await;
        let elapsed = start.elapsed();

        match o {
            Ok(_) => assert!(false),
            Err(super::EventLoopError::Timeout(_)) => assert!(true), 
            Err(_) => assert!(false)
        }

        assert!(elapsed.as_secs() == 5);
    }

    #[test]
    fn disconnection_errors_honor_reconnection_options() {

    }

    async fn throttled_requests_works_with_correct_delays_between_requests() {
        let (requests_tx, requests_rx) = channel(5);
        let (mut eventloop, tx, mut rx) = eventloop(requests_rx);
        let mut eventloop = eventloop.mqtt();

        thread::spawn(move || {
            requests(requests_tx);
        });

        for _ in 0..10 {
            dbg!();
            let _notification = eventloop.next().await;
            dbg!();
            let packet = rx.next().await.unwrap();
            println!("Packet = {:?}", packet);
        }
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


    struct IO {
        tx: Sender<Vec<u8>>,
        rx: Receiver<Vec<u8>>
    }


    impl IO {
        pub fn new() -> (IO, Sender<Vec<u8>>, Receiver<Vec<u8>>) {
            let (client_tx, server_rx) = channel(10);
            let (server_tx, client_rx) = channel(10);
            let io = IO {tx: client_tx, rx: client_rx};
            (io, server_tx, server_rx)
        }
    }


    impl AsyncRead for IO {
        fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
            match self.get_mut().rx.poll_recv(cx) {
                Poll::Ready(Some(d)) => {
                    dbg!(d.len(), buf.len());
                    buf.clone_from_slice(&d);
                    Poll::Ready(Ok(buf.len()))
                }
                Poll::Ready(None) => {
                    dbg!();
                    Poll::Ready(Ok(0))
                }
                Poll::Pending => Poll::Pending,
            }
        } 
    }

    impl AsyncWrite for IO {
        fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize, io::Error>> {
            let f = self.get_mut().tx.send(buf.to_vec());
            pin_mut!(f);

            match f.poll(cx) {
                Poll::Ready(_) => Poll::Ready(Ok(buf.len())),
                Poll::Pending => Poll::Pending
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }
}
