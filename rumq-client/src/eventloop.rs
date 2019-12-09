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
use crate::{MqttOptions, ReconnectOptions};

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

/// Connects to the server and returs an object which encompasses state of the connection.
/// Use this to create an `stream` and poll it with tokio 
/// The choice of separating `MqttEventLoop` and `stream` methods is to get access to the
/// internal state after the work with the stream is done. This is useful in scenarios like
/// shutdown where the current state should be persisted and passed back to the `stream` as
/// a `Stream`
/// First connection is done here instead of the stream to initialize network directly without
/// Option and to have good error code for failured. If using an encapsulated struct with
/// connection parameters (with state inside `MqttEventLoop`) and handling indermediate critical
/// errors (like intermediate authorization failure after initial success)
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
    /// This stream handle mqtt state and reconnections. It's critical to define when to retry
    /// and when to return error to the user. For example, intermediate mqtt auth failure should
    /// be classified as hard error and retry shouldn't be performed. Same with TLS auth failures
    /// Other soft error like server/intermediate node going down intemediately or should be 
    /// either retried infinitely or number of times asked by the user.
    /// Any hard errors are yielded as last `Notification::Error` element of the stream before
    /// closing the stream. These can be converted to finer versions when necessary
    /// There are no methods to poll the stream for the user. This is done to prevent extra copies
    /// of user notifcations (if a channel is used)
    /// NOTE: For cases where the steam should be inturrepted, user's should wrap the stream into
    /// an interruptible stream. check `stream-cancel` for example. Usecases like shutdown can
    /// depend on this to force stop the stream and use `MqttState` in `MqttEventLoop` to persist
    /// it to disk
    /// NOTE: Similary stream can be paused in the stream loop by user to pause the stream
    /// TODO: Differentiate TLS auth errors and server being down
    /// TODO: User requests which are bounded streams (ends after producing 'n' elements) or channels
    /// which are closed before acks aren't received, the current implementation ends the mqtt stream 
    /// immediately. Probably the stream should end when all the state buffers are acked with a timeout
    pub fn stream(&mut self, reconnect: ReconnectOptions) -> impl Stream<Item = Notification> + '_ {
        let o = stream! {
            'main: loop {
                // loop which polls mqtt requests and network after a connection is established
                'stream: loop {
                    let (notification, reply) = match self.read_network_and_requests().await {
                        Ok(o) => o,
                        Err(EventLoopError::NoRequest) => {
                            let error = format!("RequestStreamClosed");
                            yield Notification::Error(error);
                            break 'main
                        }
                        Err(e) => {
                            let error = format!("{:?}", e);
                            yield Notification::Error(error);
                            break 'stream
                        }
                    };

                    // write the reply back to the network
                    if let Some(p) = reply {
                        match self.network.mqtt_write(&p).await {
                            Ok(_) => (),
                            Err(e) => {
                                let error = format!("{:?}", e);
                                yield Notification::Error(error);
                                break 'stream
                            }
                        }
                    }

                    // yield the notification to the user
                    if let Some(n) = notification { 
                        yield n 
                    }
                }

                // connection retry
                'reconnection: loop {
                    let mut network = match network_connect(&self.options).await {
                        Ok(network) => network,
                        Err(e) => {
                            error!("Network connection error = {:?}", e);
                            match reconnect {
                                ReconnectOptions::Never => {
                                    let err = format!("Network connection error = {:?}", e);
                                    yield Notification::Error(err);
                                    break 'main
                                }
                                ReconnectOptions::Always(sleep) => {
                                    time::delay_for(sleep).await;
                                    continue 'reconnection 
                                }
                                _ => unimplemented!()
                            }
                        }
                    };
                    
                    if let Err(e) = mqtt_connect(&self.options, &mut network, &mut self.state).await {
                        let error = format!("{:?}", e);
                        yield Notification::Error(error);
                        break 'main
                    }

                    // make network and user requests generic for better unit testing capabilities
                    let network = Box::new(network);
                    self.network = network;
                    continue 'main
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
    use crate::{Request, MqttOptions, ReconnectOptions};

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
        let mut eventloop = eventloop.stream(ReconnectOptions::Never);

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
