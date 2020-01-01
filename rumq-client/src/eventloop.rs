use crate::{Notification, Request, network};
use derive_more::From;
use rumq_core::{self, Packet, MqttRead, MqttWrite};
use futures_util::{select, pin_mut, FutureExt};
use futures_util::stream::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{self, Elapsed};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use async_stream::stream;
use pin_project::pin_project;
use crate::state::{StateError, MqttState};
use crate::MqttOptions;

use std::io;
use std::time::Duration;

pub struct MqttEventLoop {
    runtime: Option<Runtime>
}

pub struct Runtime {
    pub state: MqttState,
    pub options: MqttOptions,
    pub requests: Box<dyn Requests>,
}

#[derive(From, Debug)]
pub enum EventLoopError {
    Io(io::Error),
    RequestStreamDone,
    MqttState(StateError),
    Timeout(Elapsed),
    Rumq(rumq_core::Error),
    Network(network::Error)
}

/// Returns an object which encompasses state of the connection.
/// Use this to create an `Stream` with `stream()` method and poll it with tokio 
/// The choice of separating `MqttEventLoop` and `stream` methods is to get access to the
/// internal state and mqtt options after the work with the `Stream` is done or stopped. 
/// This is useful in scenarios like shutdown where the current state should be persisted or
/// during reconnection when the state from last disconnection should be resumend.
/// For a similar reason, requests are also initialized as part of this method to reuse same 
/// request stream while retrying after the previous `Stream` from `stream()` method ends
/// ```ignore
/// let mut eventloop = eventloop(options, requests);
/// loop {
///     let mut stream = eventloop.stream(reconnection_options);
///     while let Some(notification) = stream.next().await() {}
/// }
/// ```
/// When mqtt `stream` ends due to critical errors (like auth failure), user has a choice to 
/// access and update `options`, `state` and `requests`.
/// For example, state and requests can be used to save state to disk before shutdown.
/// Options can be used to update gcp iotcore password
/// TODO: Remove `mqttoptions` from `state` to make sure that there is not chance of dirty opts
pub fn eventloop(options: MqttOptions, requests: impl Requests + 'static) -> MqttEventLoop {
    let state = MqttState::new(options.clone());
    let requests = Box::new(requests);

    let  runtime = Runtime {
        state,
        options,
        requests,
    };

    let eventloop = MqttEventLoop { runtime: Some(runtime) };
    eventloop
}

fn stream(mut runtime: Runtime) -> impl Stream<Item = Notification> {
    stream! {
        let mut network = match runtime.connect().await {
            Ok(network) => network,
            Err(e) => {
                yield Notification::Error(e);
                return
            }
        };

        // let mut network_stream = network_stream(runtime.options.keep_alive, &mut network);
        // let mut request_stream = request_stream(runtime.options.keep_alive, runtime.options.throttle, &mut runtime.requests);

        pin_mut!(network);
        
        loop {
            let (notification, outpacket) = select! {
                o = network.mqtt_read().fuse() => runtime.handle_packet(o.unwrap()).await.unwrap(),
                o = runtime.requests.next().fuse() => runtime.handle_request(o.unwrap()).await.unwrap(),
            };
            
            // write the reply back to the network
            if let Some(p) = outpacket {
                if let Err(e) = network.mqtt_write(&p).await {
                    yield Notification::Error(e.into());
                    break
                }
            }

            // yield the notification to the user
            if let Some(n) = notification { yield n }
        }
    }
}

fn request_stream<R: Requests>(keep_alive: Duration, throttle: Duration, requests: R) -> impl Stream<Item = Packet> {
    stream! {
        let mut requests = time::throttle(throttle, requests);

        loop {
            let timeout_request = time::timeout(keep_alive, async {
                let request = requests.next().await;
                request
            }).await;


            match timeout_request {
                Ok(Some(request)) => yield request.into(),
                Ok(None) => break,
                Err(_) => {
                    let packet = Packet::Pingreq;
                    yield packet
                }
            }
        }
    }
}

fn network_stream<S: Network>(keep_alive: Duration, mut network: S) -> impl Stream<Item = Packet> {
    stream! {
        loop {
            let timeout_packet = time::timeout(keep_alive, async {
                let packet = network.mqtt_read().await;
                packet
            }).await;

            let packet = match timeout_packet {
                Ok(p) => p,
                Err(_) => {
                    yield Packet::Pingreq;
                    continue
                }
            };

            match packet {
                Ok(packet) => yield packet,
                Err(_) => break 
            }
        }
    }
}

impl Runtime {
    async fn connect(&mut self) -> Result<Box<dyn Network>, EventLoopError> {
        let mut network = self.network_connect().await?;
        self.mqtt_connect(&mut network).await?;

        Ok(network)
    }

    async fn network_connect(&self) -> Result<Box<dyn Network>, EventLoopError> {
        let network= time::timeout(Duration::from_secs(5), async {
            if self.options.ca.is_some() {
                let o = network::tls_connect(&self.options).await?;
                let o = Box::new(o);
                Ok::<Box<dyn Network>, EventLoopError>(o)
            } else {
                let o = network::tcp_connect(&self.options).await?;
                let o = Box::new(o);
                Ok::<Box<dyn Network>, EventLoopError>(o)
            }
        }).await??;

        Ok(network)
    }


    async fn mqtt_connect(&mut self, mut network: impl Network) -> Result<(), EventLoopError> {
        let id = self.options.client_id();
        let keep_alive = self.options.keep_alive().as_secs() as u16;
        let clean_session = self.options.clean_session();

        let mut connect = rumq_core::connect(id);
        connect.set_keep_alive(keep_alive).set_clean_session(clean_session);

        if let Some((username, password)) = self.options.credentials() {
            connect.set_username(username).set_password(password);
        }

        // mqtt connection with timeout
        time::timeout(Duration::from_secs(5), async {
            network.mqtt_write(&Packet::Connect(connect)).await?;
            self.state.handle_outgoing_connect()?;
            Ok::<_, EventLoopError>(())
        }).await??;

        // wait for 'timeout' time to validate connack
        time::timeout(Duration::from_secs(5), async {
            let packet = network.mqtt_read().await?;
            self.state.handle_incoming_connack(packet)?;
            Ok::<_, EventLoopError>(())
        }).await??;

        Ok(())
    }

    async fn handle_packet(&mut self, packet: Packet) -> Result<(Option<Notification>, Option<Packet>), EventLoopError> {
        let (notification, reply) = self.state.handle_incoming_mqtt_packet(packet)?;
        Ok((notification, reply))
    }

    async fn handle_request(&mut self, request: Request) -> Result<(Option<Notification>, Option<Packet>),  EventLoopError> {
        // outgoing packet handle is only user for requests, not replys. this ensures
        // ping debug print show last request time, not reply time
        let request = self.state.handle_outgoing_mqtt_packet(request.into());
        let request = Some(request);
        let notification = None;
        Ok((notification, request))
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

trait Network: AsyncWrite + AsyncRead + Unpin + Send {}
impl<T> Network for T where T: AsyncWrite + AsyncRead + Unpin + Send {}

pub trait Requests: Stream<Item = Request> + Unpin + Send + Sync {}
impl<T> Requests for T where T: Stream<Item = Request> + Unpin + Send + Sync {}


#[cfg(test)]
mod test {
    use rumq_core::*;
    use tokio::sync::mpsc::{channel, Sender, Receiver};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::{time, task};
    use futures_util::stream::StreamExt;
    use std::time::{Instant, Duration};
    use crate::{Request, MqttOptions};

    async fn start_requests(mut requests_tx: Sender<Request>) {
        for i in 0..10 {
            let topic = "hello/world".to_owned();
            let payload = vec![1, 2, 3, i];

            let publish = publish(topic, payload);
            let request = Request::Publish(publish);
            let _ = requests_tx.send(request).await;
        }
    }


    #[tokio::test]
    async fn connection_should_timeout_on_time() {
        let (_requests_tx, requests_rx) = channel(5);

        task::spawn(async move {
            let _broker = broker(1880).await;
            time::delay_for(Duration::from_secs(10)).await;
        });

        time::delay_for(Duration::from_secs(1)).await;
        let options = MqttOptions::new("dummy", "127.0.0.1", 1880);
        let mut eventloop = super::eventloop(options, requests_rx); 

        let start = Instant::now(); 
        let o = eventloop.connect().await;
        let elapsed = start.elapsed();

        match o {
            Ok(_) => assert!(false),
            Err(super::EventLoopError::Timeout(_)) => assert!(true), 
            Err(_) => assert!(false)
        }

        assert_eq!(elapsed.as_secs(), 5);
    }


    #[tokio::test]
    async fn throttled_requests_works_with_correct_delays_between_requests() {
        let mut options = MqttOptions::new("dummy", "127.0.0.1", 1881);
        options.set_throttle(Duration::from_secs(1));
        let options2 = options.clone();

        // start sending requests
        let (requests_tx, requests_rx) = channel(5);
        task::spawn(async move {
            start_requests(requests_tx).await;
        });

        // start the eventloop
        task::spawn(async move {
            time::delay_for(Duration::from_secs(1)).await;
            let mut eventloop = super::eventloop(options, requests_rx); 
            let mut stream = eventloop.stream();

            while let Some(_) = stream.next().await {}
        });


        let mut broker = broker(1881).await;
        // check incoming rate at th broker
        for i in 0..10 {
            let start = Instant::now();
            let packet = broker.mqtt_read().await.unwrap(); 
            match packet {
                Packet::Connect(_) => broker.mqtt_write(&Packet::Connack(connack(ConnectReturnCode::Accepted, false))).await.unwrap(),
                Packet::Publish(_) => {
                    let elapsed = start.elapsed();
                    if i > 1 { 
                        assert_eq!(elapsed.as_secs(), options2.throttle.as_secs())
                    }
                }
                packet => panic!("Invalid packet = {:?}", packet)
            };
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

    #[test]
    fn reconnection_resumes_from_the_previous_state() {


    }

    async fn broker(port: u16) -> TcpStream {
        let addr = format!("127.0.0.1:{}", port);
        let mut listener = TcpListener::bind(&addr).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        socket
    }
}
