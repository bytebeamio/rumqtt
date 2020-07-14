//! A pure rust MQTT client which strives to be robust, efficient and easy to use.
//! This library is backed by an async (tokio) eventloop which handles all the robustness and
//! and efficiency parts of MQTT but naturally fits into both sync and async worlds as we'll see
//!
//! Let's jump into examples right away
//!
//! A simple synchronous publish and subscribe
//! ----------------------------
//!
//! ```no_run
//!use rumqttc::{MqttOptions, Client, QoS};
//!use std::time::Duration;
//!use std::thread;
//!
//! fn main() {
//!     let mut mqttoptions = MqttOptions::new("rumqtt-sync-client", "test.mosquitto.org", 1883);
//!     mqttoptions.set_keep_alive(5);
//!
//!     let (mut client, mut connection) = Client::new(mqttoptions, 10);
//!     client.subscribe("hello/rumqtt", QoS::AtMostOnce).unwrap();
//!     thread::spawn(move || for i in 0..10 {
//!        client.publish("hello/rumqtt", QoS::AtLeastOnce, false, vec![i; i as usize]).unwrap();
//!        thread::sleep(Duration::from_millis(100));
//!     });
//!
//!     // Iterate to poll the eventloop for connection progress
//!     for (i, notification) in connection.iter().enumerate() {
//!         println!("Notification = {:?}", notification);
//!     }
//! }
//! ```
//!
//! What's happening behind the scenes
//! - Eventloop orchestrates user requests and incoming packets concurrently and hadles the state
//! - Ping the broker when necessary and detects client side half open connections as well
//! - Throttling of outgoing packets
//! - Queue size based flow control on outgoing packets
//! - Automatic reconnections
//! - Natural backpressure to the client during slow network
//!
//! In short, everything necessary to maintain a robust connection
//!
//! **NOTE**: Looping on `connection.iter()` is necessary to run the eventloop. It yields both
//! incoming and outgoing activity notifications which allows customization as user sees fit.
//! Blocking here will block connection progress
//!
//! A simple asynchronous publish and subscribe
//! ------------------------------
//! ```no_run
//! use rumqttc::{MqttOptions, Request, EventLoop};
//! use std::time::Duration;
//! use std::error::Error;
//!
//! #[tokio::main(core_threads = 1)]
//! async fn main() {
//!     let mut mqttoptions = MqttOptions::new("rumqtt-async", "test.mosquitto.org", 1883);
//!     let mut eventloop = EventLoop::new(mqttoptions, 10).await;
//!     let requests_tx = eventloop.handle();
//!
//!     loop {
//!         let notification = eventloop.poll().await.unwrap();
//!         println!("Received = {:?}", notification);
//!         tokio::time::delay_for(Duration::from_secs(1)).await;
//!     }
//! }
//! ```
//! - Reconnects if polled again after an error
//! - User handle to send requests is just a channel
//!
//! Since eventloop is externally polled (with `iter()/poll()`) out side the library, users can
//! - Distribute incoming messages based on topics
//! - Stop it when required
//! - Access internal state for use cases like graceful shutdown

#[macro_use]
extern crate log;

use std::time::Duration;

mod client;
mod tls;
mod framed;
mod state;
mod eventloop;
#[cfg(feature = "passthrough")]
mod eventloop2;

#[cfg(feature = "passthrough")]
pub use framed::Network;
pub use client::{Client, Connection, ClientError};
pub use eventloop::{ConnectionError, EventLoop};
pub use state::MqttState;
pub use mqtt4bytes::*;

#[derive(Debug, Clone)]
pub enum Incoming {
    /// Connection successful
    Connected,
    /// Incoming publish from the broker
    Publish(Publish),
    /// Incoming puback from the broker
    PubAck(PubAck),
    /// Incoming pubrec from the broker
    PubRec(PubRec),
    /// Incoming pubrel
    PubRel(PubRel),
    /// Incoming pubcomp from the broker
    PubComp(PubComp),
    /// Incoming subscribe packet
    Subscribe(Subscribe),
    /// Incoming suback from the broker
    SubAck(SubAck),
    /// Incoming unsubscribe
    Unsubscribe(Unsubscribe),
    /// Incoming unsuback from the broker
    UnsubAck(UnsubAck),
    /// Ping request
    PingReq,
    /// Ping response
    PingResp,
    /// Disconnect packet
    Disconnect,
}

/// Current outgoing activity on the eventloop
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Outgoing {
    /// Publish packet with packet identifier. 0 implies QoS 0
    Publish(u16),
    /// Publishes
    Publishes(Vec<u16>),
    /// Subscribe packet with packet identifier
    Subscribe(u16),
    /// Unsubscribe packet with packet identifier
    Unsubscribe(u16),
    /// PubAck packet
    PubAck(u16),
    /// PubAck packet
    PubAcks(Vec<u16>),
    /// PubRec packet
    PubRec(u16),
    /// PubComp packet
    PubComp(u16),
    /// Ping request packet
    PingReq,
    /// Disconnect packet
    Disconnect,
    /// Notification if requests are internally batched
    Batch
}

/// Requests by the client to mqtt event loop. Request are
/// handled one by one. This is a duplicate of possible MQTT
/// packets along with the ability to tag data and do bulk
/// operations.
/// Upcoming feature: When 'manual' feature is turned on
/// provides the ability to reply with acks when the user sees fit
#[derive(Debug)]
pub enum Request {
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubComp(PubComp),
    PubRel(PubRel),
    PingReq,
    PingResp,
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    Disconnect,
    Publishes(Vec<Publish>),
    PubAcks(Vec<PubAck>),
}

/// Key type for TLS authentication
#[derive(Debug, Copy, Clone)]
pub enum Key{
    RSA,
    ECC,
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
    /// Max internal request batching
    max_request_batch: usize,
    /// Minimum delay time between consecutive outgoing packets
    /// while retransmitting pending packets
    pending_throttle: Duration,
    /// maximum number of outgoing inflight messages
    inflight: u16,
    /// Last will that will be issued on unexpected disconnect
    last_will: Option<LastWill>,
    /// Key type for TLS 
    key_type: Key,
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
            max_request_batch: 0,
            pending_throttle: Duration::from_micros(0),
            inflight: 100,
            last_will: None,
            key_type: Key::RSA,
        }
    }

    /// Broker address
    pub fn broker_address(&self) -> (String, u16) {
        (self.broker_addr.clone(), self.port)
    }

    pub fn set_last_will(&mut self, will: LastWill) -> &mut Self {
        self.last_will = Some(will);
        self
    }

    pub fn last_will(&mut self) -> Option<LastWill> {
        self.last_will.clone()
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

    /// Maximum internal batching of requests
    pub fn set_max_request_batch(&mut self, max: usize) -> &mut Self {
        self.max_request_batch = max;
        self
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
    pub fn set_pending_throttle(&mut self, duration: Duration) -> &mut Self {
        self.pending_throttle = duration;
        self
    }

    /// Outgoing message rate
    pub fn pending_throttle(&self) -> Duration {
        self.pending_throttle
    }

    /// Set number of concurrent in flight messages
    pub fn set_inflight(&mut self, inflight: u16) -> &mut Self {
        if inflight == 0 {
            panic!("zero in flight is not allowed")
        }

        self.inflight = inflight;
        self
    }

    /// Number of concurrent in flight messages
    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    /// Use this setter to modify key_type enum, by default RSA
    pub fn set_key_type(&mut self, key_type: Key) -> &mut Self {
        self.key_type = key_type;
        self
    }

    /// get key type
    pub fn get_key_type(&self) -> Key{
        self.key_type
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
