use crate::framed::AsyncReadWrite;
use crate::NetworkOptions;
use async_trait::async_trait;
use std::io;

/// A pluggable source for the base byte stream the MQTT client communicates over.
///
/// By default `rumqttc` creates its connection with [`tokio::net`]. Implementing this
/// trait and installing it via [`NetworkOptions::set_connector`](crate::NetworkOptions::set_connector)
/// lets you run the client over an arbitrary transport instead. This is primarily
/// useful for testing under a simulated network such as
/// [`turmoil`](https://github.com/tokio-rs/turmoil).
///
/// When a connector is installed it fully owns socket creation, so it **replaces**
/// the default TCP socket (and the `proxy` setting is bypassed). The configured
/// [`NetworkOptions`] are handed to [`connect`](Connector::connect), so an
/// implementation can honor the relevant low-level settings (nodelay, send/recv
/// buffer sizes, `bind_addr`, `bind_device`) where they apply to its transport.
/// TLS and WebSocket transports still layer on top of the stream the connector
/// returns.
///
/// The connector is **not** used for `Transport::Unix`, which always connects
/// through a local Unix domain socket.
///
/// # Example
///
/// ```ignore
/// use rumqttc::{async_trait, AsyncReadWrite, Connector, NetworkOptions};
/// use std::io;
///
/// struct TurmoilConnector;
///
/// #[async_trait]
/// impl Connector for TurmoilConnector {
///     async fn connect(
///         &self,
///         addr: &str,
///         _network_options: &NetworkOptions,
///     ) -> io::Result<Box<dyn AsyncReadWrite>> {
///         let stream = turmoil::net::TcpStream::connect(addr).await?;
///         Ok(Box::new(stream))
///     }
/// }
/// ```
#[async_trait]
pub trait Connector: Send + Sync {
    /// Establish the base byte stream to the broker.
    ///
    /// `addr` is the `"host:port"` string the client would otherwise hand to
    /// [`tokio::net::lookup_host`]. The implementor is responsible for its own
    /// name resolution and socket creation.
    ///
    /// `network_options` carries the low-level connection settings configured on
    /// the event loop; the implementation may apply whichever are relevant to its
    /// transport (see the getters on [`NetworkOptions`]).
    async fn connect(
        &self,
        addr: &str,
        network_options: &NetworkOptions,
    ) -> io::Result<Box<dyn AsyncReadWrite>>;
}
