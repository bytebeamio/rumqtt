//! A pure rust MQTT client which strives to be robust, efficient and easy to use.
//! This library is backed by an async (tokio) eventloop which handles all the
//! robustness and and efficiency parts of MQTT but naturally fits into both sync
//! and async worlds as we'll see
//!
//! Let's jump into examples right away
//!
//! A simple synchronous publish and subscribe
//! ----------------------------
//!
//! ```no_run
//! use rumqttc::{MqttOptions, Client, QoS};
//! use std::time::Duration;
//! use std::thread;
//!
//! let mut mqttoptions = MqttOptions::new("rumqtt-sync", "test.mosquitto.org", 1883);
//! mqttoptions.set_keep_alive(5);
//!
//! let (mut client, mut connection) = Client::new(mqttoptions, 10);
//! client.subscribe("hello/rumqtt", QoS::AtMostOnce).unwrap();
//! thread::spawn(move || for i in 0..10 {
//!    client.publish("hello/rumqtt", QoS::AtLeastOnce, false, vec![i; i as usize]).unwrap();
//!    thread::sleep(Duration::from_millis(100));
//! });
//!
//! // Iterate to poll the eventloop for connection progress
//! for (i, notification) in connection.iter().enumerate() {
//!     println!("Notification = {:?}", notification);
//! }
//! ```
//!
//! A simple asynchronous publish and subscribe
//! ------------------------------
//!
//! ```no_run
//! use rumqttc::{MqttOptions, AsyncClient, QoS};
//! use tokio::{task, time};
//! use std::time::Duration;
//! use std::error::Error;
//!
//! # #[tokio::main(worker_threads = 1)]
//! # async fn main() {
//! let mut mqttoptions = MqttOptions::new("rumqtt-async", "test.mosquitto.org", 1883);
//! mqttoptions.set_keep_alive(5);
//!
//! let (mut client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
//! client.subscribe("hello/rumqtt", QoS::AtMostOnce).await.unwrap();
//!
//! task::spawn(async move {
//!     for i in 0..10 {
//!         client.publish("hello/rumqtt", QoS::AtLeastOnce, false, vec![i; i as usize]).await.unwrap();
//!         time::sleep(Duration::from_millis(100)).await;
//!     }
//! });
//!
//! loop {
//!     let notification = eventloop.poll().await.unwrap();
//!     println!("Received = {:?}", notification);
//!     tokio::time::sleep(Duration::from_secs(1)).await;
//! }
//! # }
//! ```
//!
//! Quick overview of features
//! - Eventloop orchestrates outgoing/incoming packets concurrently and hadles the state
//! - Pings the broker when necessary and detects client side half open connections as well
//! - Throttling of outgoing packets (todo)
//! - Queue size based flow control on outgoing packets
//! - Automatic reconnections by just continuing the `eventloop.poll()/connection.iter()` loop`
//! - Natural backpressure to client APIs during bad network
//! - Immediate cancellation with `client.cancel()`
//!
//! In short, everything necessary to maintain a robust connection
//!
//! Since the eventloop is externally polled (with `iter()/poll()` in a loop)
//! out side the library and `Eventloop` is accessible, users can
//! - Distribute incoming messages based on topics
//! - Stop it when required
//! - Access internal state for use cases like graceful shutdown or to modify options before reconnection
//!
//! ## Important notes
//!
//! - Looping on `connection.iter()`/`eventloop.poll()` is necessary to run the
//!   event loop and make progress. It yields incoming and outgoing activity
//!   notifications which allows customization as you see fit.
//!
//! - Blocking inside the `connection.iter()`/`eventloop.poll()` loop will block
//!   connection progress.
//!
//! ## FAQ
//! **Connecting to a broker using raw ip doesn't work**
//!
//! You cannot create a TLS connection to a bare IP address with a self-signed
//! certificate. This is a [limitation of rustls](https://github.com/ctz/rustls/issues/184).
//! One workaround, which only works under *nix/BSD-like systems, is to add an
//! entry to wherever your DNS resolver looks (e.g. `/etc/hosts`) for the bare IP
//! address and use that name in your code.

#[macro_use]
extern crate log;

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

mod client;
mod eventloop;
mod framed;
mod state;
mod tls;

pub use async_channel::{SendError, Sender, TrySendError};
pub use client::{AsyncClient, Client, ClientError, Connection};
pub use eventloop::{ConnectionError, Event, EventLoop};
pub use mqtt4bytes::*;
pub use state::{MqttState, StateError};
pub use tokio_rustls::rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
pub use tokio_rustls::rustls::ClientConfig;

pub type Incoming = Packet;

/// Current outgoing activity on the eventloop
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Outgoing {
    /// Publish packet with packet identifier. 0 implies QoS 0
    Publish(u16),
    /// Subscribe packet with packet identifier
    Subscribe(u16),
    /// Unsubscribe packet with packet identifier
    Unsubscribe(u16),
    /// PubAck packet
    PubAck(u16),
    /// PubRec packet
    PubRec(u16),
    /// PubRel packet
    PubRel(u16),
    /// PubComp packet
    PubComp(u16),
    /// Ping request packet
    PingReq,
    /// Ping response packet
    PingResp,
    /// Disconnect packet
    Disconnect,
}

/// Requests by the client to mqtt event loop. Request are
/// handled one by one.
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    Publish(Publish),
    PublishRaw(PublishRaw),
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
}

/// Key type for TLS authentication
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Key {
    RSA,
    ECC,
}

impl From<Publish> for Request {
    fn from(publish: Publish) -> Request {
        Request::Publish(publish)
    }
}

impl From<Subscribe> for Request {
    fn from(subscribe: Subscribe) -> Request {
        Request::Subscribe(subscribe)
    }
}

impl From<Unsubscribe> for Request {
    fn from(unsubscribe: Unsubscribe) -> Request {
        Request::Unsubscribe(unsubscribe)
    }
}

/// Client authentication option for mqtt connect packet
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecurityOptions {
    /// No authentication.
    None,
    /// Use the specified `(username, password)` tuple to authenticate.
    UsernamePassword(String, String),
}

#[derive(Clone)]
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
    /// maximum incoming packet size (verifies remaining length of the packet)
    max_incoming_packet_size: usize,
    /// Maximum outgoing packet size (only verifies publish payload size)
    // TODO Verify this with all packets. This can be packet.write but message left in
    // the state might be a footgun as user has to explicitly clean it. Probably state
    // has to be moved to network
    max_outgoing_packet_size: usize,
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
    /// Injected rustls ClientConfig for TLS, to allow more customisation.
    tls_client_config: Option<Arc<ClientConfig>>,
    /// Enabling will wait for incoming packets to avoid collisions
    collision_safety: bool,
    conn_timeout: u64,
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
            max_incoming_packet_size: 10 * 1024,
            max_outgoing_packet_size: 10 * 1024,
            request_channel_capacity: 10,
            max_request_batch: 0,
            pending_throttle: Duration::from_micros(0),
            inflight: 100,
            last_will: None,
            key_type: Key::RSA,
            tls_client_config: None,
            collision_safety: false,
            conn_timeout: 5,
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

    pub fn last_will(&self) -> Option<LastWill> {
        self.last_will.clone()
    }

    /// Set the CA certificate to use for TLS connections. Doing so implicitly enables TLS.
    ///
    /// See `set_client_auth`, `set_alpn` and `set_key_type`. If you want to control more options
    /// then you can inject a rustls ClientConfig directly with `set_tls_client_config`.
    pub fn set_ca(&mut self, ca: Vec<u8>) -> &mut Self {
        assert!(
            self.tls_client_config.is_none(),
            "Can't set both tls_client_config and ca."
        );
        self.ca = Some(ca);
        self
    }

    pub fn ca(&self) -> Option<Vec<u8>> {
        self.ca.clone()
    }

    pub fn set_client_auth(&mut self, cert: Vec<u8>, key: Vec<u8>) -> &mut Self {
        assert!(
            self.tls_client_config.is_none(),
            "Can't set both tls_client_config and ca."
        );
        self.client_auth = Some((cert, key));
        self
    }

    pub fn client_auth(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.client_auth.clone()
    }

    pub fn set_alpn(&mut self, alpn: Vec<Vec<u8>>) -> &mut Self {
        assert!(
            self.tls_client_config.is_none(),
            "Can't set both tls_client_config and alpn."
        );
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

    /// Set packet size limit for outgoing an incoming packets
    pub fn set_max_packet_size(&mut self, incoming: usize, outgoing: usize) -> &mut Self {
        self.max_incoming_packet_size = incoming;
        self.max_outgoing_packet_size = outgoing;
        self
    }

    /// Maximum packet size
    pub fn max_packet_size(&self) -> usize {
        self.max_incoming_packet_size
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
    pub fn get_key_type(&self) -> Key {
        self.key_type
    }

    /// Inject a rustls ClientConfig to use for making a TLS connection.
    ///
    /// Doing so implicitly enabled TLS. You must not set both this and the other TLS options
    /// (set_ca, set_client_auth, set_alpn)
    pub fn set_tls_client_config(&mut self, tls_client_config: Arc<ClientConfig>) -> &mut Self {
        assert!(
            self.ca.is_none(),
            "Can't set both tls_client_config and ca."
        );
        assert!(
            self.client_auth.is_none(),
            "Can't set both tls_client_config and client_auth."
        );
        assert!(
            self.alpn.is_none(),
            "Can't set both tls_client_config and alpn."
        );
        self.tls_client_config = Some(tls_client_config);
        self
    }

    /// Get the ClientConfig which was previously set, if any.
    pub fn tls_client_config(&self) -> Option<Arc<ClientConfig>> {
        self.tls_client_config.clone()
    }

    pub fn set_collision_safety(&mut self, c: bool) -> &mut Self {
        self.collision_safety = c;
        self
    }

    pub fn collision_safety(&self) -> bool {
        self.collision_safety
    }

    /// set connection timeout in secs
    pub fn set_connection_timeout(&mut self, timeout: u64) -> &mut Self {
        self.conn_timeout = timeout;
        self
    }

    /// get timeout in secs
    pub fn connection_timeout(&self) -> u64 {
        self.conn_timeout
    }
}

// Implement Debug manually because ClientConfig doesn't implement it, so derive(Debug) doesn't
// work.
impl Debug for MqttOptions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("MqttOptions")
            .field("broker_addr", &self.broker_addr)
            .field("port", &self.port)
            .field("keep_alive", &self.keep_alive)
            .field("clean_session", &self.clean_session)
            .field("client_id", &self.client_id)
            .field("ca", &self.ca)
            .field("client_auth", &self.client_auth)
            .field("alpn", &self.alpn)
            .field("credentials", &self.credentials)
            .field("max_packet_size", &self.max_incoming_packet_size)
            .field("request_channel_capacity", &self.request_channel_capacity)
            .field("max_request_batch", &self.max_request_batch)
            .field("pending_throttle", &self.pending_throttle)
            .field("inflight", &self.inflight)
            .field("last_will", &self.last_will)
            .field("key_type", &self.key_type)
            .field(
                "tls_client_config",
                &self.tls_client_config.as_ref().map(|_| "..."),
            )
            .field("conn_timeout", &self.conn_timeout)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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

    #[test]
    #[should_panic]
    fn ca_and_client_config() {
        let client_config = ClientConfig::new();
        let mut mqtt_opts = MqttOptions::new("client", "127.0.0.1", 1883);
        mqtt_opts
            .set_tls_client_config(Arc::new(client_config))
            .set_ca(vec![]);
    }
}
