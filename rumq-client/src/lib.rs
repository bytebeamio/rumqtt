//! A pure rust mqtt client which strives to be robust, efficient and easy to use.
//! * Eventloop is just an async `Stream` which can be polled by tokio
//! * Requests to eventloop is also a `Stream`. Solves both bounded an unbounded usecases
//! * Robustness just a loop away
//! * Flexible access to the state of eventloop to control its behaviour
//!
//! Accepts any stream of Requests
//! ----------------------------
//! Build bounded, unbounded, interruptible or any other stream (that fits your need) to
//! feed the eventloop.
//!
//! **Few of our real world use cases**
//!
//! - A stream which orchestrates data between disk and memory by detecting backpressure and never (practically) loose data
//! - A stream which juggles data between several channels based on priority of the data
//!
//! ```ignore
//! #[tokio::main(core_threads = 1)]
//! async fn main() {
//!     let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
//!     let requests = Vec::new::<Request>();
//!
//!     let mut eventloop = eventloop(mqttoptions, requests_rx);
//!     let mut stream = eventloop.stream();
//!     while let Some(item) = stream.next().await {
//!         println!("Received = {:?}", item);
//!     }
//! }
//! ```
//! Robustness a loop away
//! ----------------------
//! Networks are unreliable. But robustness is easy
//!
//! - Just create a new stream from the existing eventloop
//! - Resumes from where it left
//! - Access the state of the eventloop to customize the behaviour of the next connection
//!
//! ```ignore
//! #[tokio::main(core_threads = 1)]
//! async fn main() {
//!     let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
//!     let requests = Vec::new::<Request>();
//!
//!     let mut eventloop = eventloop(mqttoptions, requests_rx);
//!
//!     // loop to reconnect and resume
//!     loop {
//!         let mut stream = eventloop.stream();
//!         while let Some(item) = stream.next().await {
//!             println!("Received = {:?}", item);
//!         }
//!         
//!         time::delay_for(Duration::from_secs(1)).await;
//!     }
//! }
//! ```
//!
//! Eventloop is just a stream which can be polled with tokio
//! ----------------------
//! - Plug it into `select!` `join!` to interleave with other streams on the the same thread
//!
//! ```ignore
//! #[tokio::main(core_threads = 1)]
//! async fn main() {
//!     let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
//!     let requests = Vec::new::<Request>();
//!
//!     let mut eventloop = eventloop(mqttoptions, requests_rx);
//!
//!     // plug it into tokio ecosystem
//!     let mut stream = eventloop.stream();
//! }
//! ```
//!
//! Powerful notification system to control the runtime
//! ----------------------
//! Eventloop stream yields all the interesting event ranging for data on the network to
//! disconnections and reconnections. Use it the way you see fit
//!
//! - Resubscribe after reconnection
//! - Stop after receiving Nth puback
//!
//! ```ignore
//! #[tokio::main(core_threads = 1)]
//! async fn main() {
//!     let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
//!     let (requests_tx, requests_rx) = channel(10);
//!
//!     let mut eventloop = eventloop(mqttoptions, requests_rx);
//!
//!     // loop to reconnect and resume
//!     loop {
//!         let mut stream = eventloop.stream();
//!         while let Some(notification) = stream.next().await {
//!             println!("Received = {:?}", item);
//!             match notification {
//!                 Notification::Connect => requests_tx.send(subscribe).unwrap(),
//!             }
//!         }
//!         
//!         time::delay_for(Duration::from_secs(1)).await;
//!     }
//! }
//! ```

#![recursion_limit = "512"]

#[macro_use]
extern crate log;

use rumq_core::mqtt4::{MqttRead, MqttWrite, Packet};
use std::io::Cursor;
use std::time::Duration;

pub(crate) mod eventloop;
pub(crate) mod network;
pub(crate) mod state;

pub use eventloop::eventloop;
pub use eventloop::{EventLoopError, MqttEventLoop};
pub use state::MqttState;

#[doc(hidden)]
pub use rumq_core::mqtt4::{publish, subscribe, Connect, PacketIdentifier, Publish, QoS, Suback, Subscribe, Unsubscribe};

/// Includes incoming packets from the network and other interesting events happening in the eventloop
#[derive(Debug)]
pub enum Notification {
    /// Successful mqtt connection
    Connected,
    /// Incoming publish from the broker
    Publish(Publish),
    /// Incoming puback from the broker
    Puback(PacketIdentifier),
    /// Incoming pubrec from the broker
    Pubrec(PacketIdentifier),
    /// Incoming pubcomp from the broker
    Pubcomp(PacketIdentifier),
    /// Incoming suback from the broker
    Suback(Suback),
    /// Incoming unsuback from the broker
    Unsuback(PacketIdentifier),
    /// Eventloop error
    StreamEnd(EventLoopError),
    /// Request stream done
    RequestsDone,
    /// Network closed by the remote
    NetworkClosed,
}

#[doc(hidden)]
/// Requests by the client to mqtt event loop. Request are
/// handle one by one#[derive(Debug)]
#[derive(Debug)]
pub enum Request {
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Reconnect(Connect),
    Disconnect,
}

impl From<Publish> for Request {
    fn from(publish: Publish) -> Request {
        return Request::Publish(publish);
    }
}

impl From<Subscribe> for Request {
    fn from(subscribe: Subscribe) -> Request {
        return Request::Subscribe(subscribe);
    }
}

impl From<Unsubscribe> for Request {
    fn from(unsubscribe: Unsubscribe) -> Request {
        return Request::Unsubscribe(unsubscribe);
    }
}

/// From implementations for serialized requests
/// TODO Probably implement for io::Result<Vec<u8>> if possible?
impl From<Request> for Vec<u8> {
    fn from(request: Request) -> Vec<u8> {
        let mut packet = Cursor::new(Vec::new());
        let o = match request {
            Request::Reconnect(connect) => packet.mqtt_write(&Packet::Connect(connect)),
            Request::Publish(publish) => packet.mqtt_write(&Packet::Publish(publish)),
            Request::Subscribe(subscribe) => packet.mqtt_write(&Packet::Subscribe(subscribe)),
            _ => unimplemented!(),
        };

        o.unwrap();
        packet.into_inner()
    }
}

impl From<Vec<u8>> for Request {
    fn from(payload: Vec<u8>) -> Request {
        let mut payload = Cursor::new(payload);
        let packet = payload.mqtt_read().unwrap();

        match packet {
            Packet::Connect(connect) => Request::Reconnect(connect),
            Packet::Publish(publish) => Request::Publish(publish),
            Packet::Subscribe(subscribe) => Request::Subscribe(subscribe),
            _ => unimplemented!(),
        }
    }
}

#[doc(hidden)]
/// Commands sent by the client to mqtt event loop. Commands
/// are of higher priority and will be `select`ed along with
/// [request]s
///
/// request: enum.Request.html
#[derive(Debug)]
pub enum Command {
    Pause,
    Resume,
}

/// Client authentication option for mqtt connect packet
#[derive(Clone, Debug)]
pub enum SecurityOptions {
    /// No authentication.
    None,
    /// Use the specified `(username, password)` tuple to authenticate.
    UsernamePassword(String, String),
}

// TODO: Should all the options be exposed as public? Drawback
// would be loosing the ability to panic when the user options
// are wrong (e.g empty client id) or aggressive (keep alive time)
/// Options to configure the behaviour of mqtt connection
#[derive(Clone, Debug)]
pub struct MqttOptions {
    /// broker address that you want to connect to
    broker_addr: String,
    /// broker port
    port: u16,
    /// keep alive time to send pingreq to broker when the connection is idle
    keep_alive: Duration,
    /// clean (or) persistent session
    clean_session: bool,
    /// client identifier
    client_id: String,
    /// connection method
    ca: Option<Vec<u8>>,
    /// tls client_authentication
    client_auth: Option<(Vec<u8>, Vec<u8>)>,
    /// alpn settings
    alpn: Option<Vec<Vec<u8>>>,
    /// username and password
    credentials: Option<(String, String)>,
    /// maximum packet size
    max_packet_size: usize,
    /// request (publish, subscribe) channel capacity
    request_channel_capacity: usize,
    /// notification channel capacity
    notification_channel_capacity: usize,
    /// Minimum delay time between consecutive outgoing packets
    throttle: Duration,
    /// maximum number of outgoing inflight messages
    inflight: usize,
}

impl MqttOptions {
    /// New mqtt options
    pub fn new<S: Into<String>, T: Into<String>>(id: S, host: T, port: u16) -> MqttOptions {
        let id = id.into();
        if id.starts_with(' ') || id.is_empty() {
            panic!("Invalid client id")
        }

        MqttOptions {
            broker_addr: host.into(),
            port,
            keep_alive: Duration::from_secs(60),
            clean_session: true,
            client_id: id,
            ca: None,
            client_auth: None,
            alpn: None,
            credentials: None,
            max_packet_size: 256 * 1024,
            request_channel_capacity: 10,
            notification_channel_capacity: 10,
            throttle: Duration::from_micros(10),
            inflight: 100,
        }
    }

    /// Broker address
    pub fn broker_address(&self) -> (String, u16) {
        (self.broker_addr.clone(), self.port)
    }

    pub fn set_ca(&mut self, ca: Vec<u8>) -> &mut Self {
        self.ca = Some(ca);
        self
    }

    pub fn ca(&self) -> Option<Vec<u8>> {
        self.ca.clone()
    }

    pub fn set_client_auth(&mut self, cert: Vec<u8>, key: Vec<u8>) -> &mut Self {
        self.client_auth = Some((cert, key));
        self
    }

    pub fn client_auth(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.client_auth.clone()
    }

    pub fn set_alpn(&mut self, alpn: Vec<Vec<u8>>) -> &mut Self {
        self.alpn = Some(alpn);
        self
    }

    pub fn alpn(&self) -> Option<Vec<Vec<u8>>> {
        self.alpn.clone()
    }

    /// Set number of seconds after which client should ping the broker
    /// if there is no other data exchange
    pub fn set_keep_alive(&mut self, secs: u16) -> &mut Self {
        if secs < 5 {
            panic!("Keep alives should be >= 5  secs");
        }

        self.keep_alive = Duration::from_secs(u64::from(secs));
        self
    }

    /// Keep alive time
    pub fn keep_alive(&self) -> Duration {
        self.keep_alive
    }

    /// Client identifier
    pub fn client_id(&self) -> String {
        self.client_id.clone()
    }

    /// Set packet size limit (in Kilo Bytes)
    pub fn set_max_packet_size(&mut self, sz: usize) -> &mut Self {
        self.max_packet_size = sz * 1024;
        self
    }

    /// Maximum packet size
    pub fn max_packet_size(&self) -> usize {
        self.max_packet_size
    }

    /// `clean_session = true` removes all the state from queues & instructs the broker
    /// to clean all the client state when client disconnects.
    ///
    /// When set `false`, broker will hold the client state and performs pending
    /// operations on the client when reconnection with same `client_id`
    /// happens. Local queue state is also held to retransmit packets after reconnection.
    pub fn set_clean_session(&mut self, clean_session: bool) -> &mut Self {
        self.clean_session = clean_session;
        self
    }

    /// Clean session
    pub fn clean_session(&self) -> bool {
        self.clean_session
    }

    /// Username and password
    pub fn set_credentials<S: Into<String>>(&mut self, username: S, password: S) -> &mut Self {
        self.credentials = Some((username.into(), password.into()));
        self
    }

    /// Security options
    pub fn credentials(&self) -> Option<(String, String)> {
        self.credentials.clone()
    }

    /// Set notification channel capacity
    pub fn set_notification_channel_capacity(&mut self, capacity: usize) -> &mut Self {
        self.notification_channel_capacity = capacity;
        self
    }

    /// Notification channel capacity
    pub fn notification_channel_capacity(&self) -> usize {
        self.notification_channel_capacity
    }

    /// Set request channel capacity
    pub fn set_request_channel_capacity(&mut self, capacity: usize) -> &mut Self {
        self.request_channel_capacity = capacity;
        self
    }

    /// Request channel capacity
    pub fn request_channel_capacity(&self) -> usize {
        self.request_channel_capacity
    }

    /// Enables throttling and sets outoing message rate to the specified 'rate'
    pub fn set_throttle(&mut self, duration: Duration) -> &mut Self {
        self.throttle = duration;
        self
    }

    /// Outgoing message rate
    pub fn throttle(&self) -> Duration {
        self.throttle
    }

    /// Set number of concurrent in flight messages
    pub fn set_inflight(&mut self, inflight: usize) -> &mut Self {
        if inflight == 0 {
            panic!("zero in flight is not allowed")
        }

        self.inflight = inflight;
        self
    }

    /// Number of concurrent in flight messages
    pub fn inflight(&self) -> usize {
        self.inflight
    }
}

#[cfg(test)]
mod test {
    use super::MqttOptions;

    #[test]
    #[should_panic]
    fn client_id_startswith_space() {
        let _mqtt_opts = MqttOptions::new(" client_a", "127.0.0.1", 1883).set_clean_session(true);
    }

    #[test]
    #[should_panic]
    fn no_client_id() {
        let _mqtt_opts = MqttOptions::new("", "127.0.0.1", 1883).set_clean_session(true);
    }
}
