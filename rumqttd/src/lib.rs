use std::collections::HashMap;
use std::path::PathBuf;

use segments::Storage;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{
    filter::EnvFilter,
    fmt::{
        format::{Format, Pretty},
        Layer,
    },
    layer::Layered,
    reload::Handle,
    Registry,
};

use std::net::SocketAddr;

mod link;
pub mod protocol;
mod router;
mod segments;
mod server;

pub type ConnectionId = usize;
pub type RouterId = usize;
pub type NodeId = usize;
pub type Topic = String;
pub type Filter = String;
pub type TopicId = usize;
pub type Offset = (u64, u64);
pub type Cursor = (u64, u64);

pub use link::local;
pub use link::meters;

pub use router::{GetMeter, Meter, Notification};
pub use server::Broker;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Config {
    pub id: usize,
    pub router: RouterConfig,
    pub v4: HashMap<String, ServerSettings>,
    pub v5: HashMap<String, ServerSettings>,
    pub ws: HashMap<String, ServerSettings>,
    pub cluster: Option<ClusterSettings>,
    pub console: ConsoleSettings,
    pub bridge: Option<BridgeConfig>,
    pub prometheus: Option<PrometheusSetting>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrometheusSetting {
    port: u16,
    // How frequently to update metrics
    interval: u64,
}

// TODO: Change names without _ until config-rs issue is resolved
// https://github.com/mehcode/config-rs/issues/369
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TlsConfig {
    Rustls {
        capath: String,
        certpath: String,
        keypath: String,
    },
    NativeTls {
        pkcs12path: String,
        pkcs12pass: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerSettings {
    pub name: String,
    pub listen: SocketAddr,
    pub tls: Option<TlsConfig>,
    pub next_connection_delay_ms: u64,
    pub connections: ConnectionSettings,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub throttle_delay_ms: u64,
    pub max_payload_size: usize,
    pub max_inflight_count: u16,
    pub max_inflight_size: usize,
    #[serde(default)]
    pub dynamic_filters: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSettings {
    /// Id with which this node connects to other nodes of the mesh
    pub node_id: NodeId,
    /// Address on which this broker is listening for mesh connections
    pub listen: String,
    /// Address of clusters that this node has to initiate connection
    pub seniors: Vec<(ConnectionId, String)>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub instant_ack: bool,
    pub max_segment_size: usize,
    pub max_segment_count: usize,
    pub max_read_len: u64,
    pub max_connections: usize,
    pub initialized_filters: Option<Vec<Filter>>,
}

type ReloadHandle = Handle<EnvFilter, Layered<Layer<Registry, Pretty, Format<Pretty>>, Registry>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConsoleSettings {
    pub listen: String,
    #[serde(skip)]
    filter_handle: Option<ReloadHandle>,
}

impl ConsoleSettings {
    pub fn set_filter_reload_handle(&mut self, handle: ReloadHandle) {
        self.filter_handle.replace(handle);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BridgeConfig {
    pub url: String,
    pub port: u16,
    pub qos: u8,
    pub sub_path: String,
    pub reconnection_delay: u64,
    pub ping_delay: u64,
    pub timeout_delay: u64,
    #[serde(default)]
    pub transport: Transport,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Transport {
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "tls")]
    Tls {
        ca: PathBuf,
        client_auth: Option<ClientAuth>,
    },
}

impl Default for Transport {
    fn default() -> Self {
        Transport::Tcp
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientAuth {
    certs: PathBuf,
    key: PathBuf,
}
