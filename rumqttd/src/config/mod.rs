use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub use serialconfig::BridgeConfig;
pub use serialconfig::ClientAuth;
pub use serialconfig::ClusterSettings;
pub use serialconfig::ConsoleSettings;
pub use serialconfig::MetricSettings;
pub use serialconfig::MetricType;
pub use serialconfig::PrometheusSetting;
pub use serialconfig::RouterConfig;
pub use serialconfig::TlsConfig;
pub use serialconfig::Transport;

use crate::protocol::Publish;
use crate::{protocol::Login, serialconfig};

/// Data associated with a connection by the authenticator. Used to authorize messages.
pub trait ConnAuthContext: Send + Sync {
    fn authorize_publish(&self, publish: &Publish) -> bool;
    fn authorize_notify(&self, publish: &Publish) -> bool;
}

/// Authenticates new connections and associates metadata with them.
pub trait Authenticator: Send + Sync {
    // Authenticate a connection.  A return value of `None` indicates to reject the connection.
    // Otherwise, the returned context is associated with the connection.
    fn authenticate(&self, login: Option<Login>) -> Option<Box<dyn ConnAuthContext>>;
}

/// Metadata to associate with a connection.
pub struct AllowConnAuthContext;

impl ConnAuthContext for AllowConnAuthContext {
    fn authorize_publish(&self, _packet: &Publish) -> bool {
        return true;
    }

    fn authorize_notify(&self, _packet: &Publish) -> bool {
        return true;
    }
}

/// Allow all connections, allow all access to all connections. Used by default if no other auth is specified.
pub struct AllowAuthenticator;

impl Authenticator for AllowAuthenticator {
    fn authenticate(&self, _login: Option<Login>) -> Option<Box<dyn ConnAuthContext>> {
        return Some(Box::new(AllowConnAuthContext));
    }
}

/// Authenticates with any of the provided usernames and passwords. All logins are provided global read and write access.
pub struct StaticAuthenticator {
    /// Map of username to password
    pub logins: HashMap<String, String>,
}

impl Authenticator for StaticAuthenticator {
    fn authenticate(&self, login: Option<Login>) -> Option<Box<dyn ConnAuthContext>> {
        let login = match login {
            Some(l) => l,
            None => return None,
        };
        let pass = match self.logins.get(&login.username) {
            Some(x) => x,
            None => return None,
        };
        if pass != &login.password {
            return None;
        }
        return Some(Box::new(AllowConnAuthContext));
    }
}

#[derive(Clone)]
pub struct Config {
    pub id: usize,
    pub router: RouterConfig,
    pub v4: HashMap<String, ServerSettings>,
    pub v5: Option<HashMap<String, ServerSettings>>,
    pub ws: Option<HashMap<String, ServerSettings>>,
    pub cluster: Option<ClusterSettings>,
    pub console: ConsoleSettings,
    pub bridge: Option<BridgeConfig>,
    pub prometheus: Option<PrometheusSetting>,
    pub metrics: Option<HashMap<MetricType, MetricSettings>>,
}

impl From<serialconfig::Config> for Config {
    fn from(value: serialconfig::Config) -> Self {
        return Config {
            id: value.id,
            router: value.router,
            v4: value.v4.into_iter().map(|(k, v)| (k, v.into())).collect(),
            v5: value
                .v5
                .map(|v| v.into_iter().map(|(k, v)| (k, v.into())).collect()),
            ws: value
                .ws
                .map(|v| v.into_iter().map(|(k, v)| (k, v.into())).collect()),
            cluster: value.cluster,
            console: value.console,
            bridge: value.bridge,
            prometheus: value.prometheus,
            metrics: value.metrics,
        };
    }
}

#[derive(Clone)]
pub struct ServerSettings {
    pub name: String,
    pub listen: SocketAddr,
    pub tls: Option<TlsConfig>,
    pub next_connection_delay_ms: u64,
    pub connections: ConnectionSettings,
}

impl From<serialconfig::ServerSettings> for ServerSettings {
    fn from(value: serialconfig::ServerSettings) -> Self {
        Self {
            name: value.name,
            listen: value.listen,
            tls: value.tls,
            next_connection_delay_ms: value.next_connection_delay_ms,
            connections: value.connections.into(),
        }
    }
}

#[derive(Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub throttle_delay_ms: u64,
    pub max_payload_size: usize,
    pub max_inflight_count: u16,
    pub max_inflight_size: usize,
    pub dynamic_filters: bool,
    pub auth: Arc<dyn Authenticator>,
}

impl From<serialconfig::ConnectionSettings> for ConnectionSettings {
    fn from(value: serialconfig::ConnectionSettings) -> Self {
        return Self {
            connection_timeout_ms: value.connection_timeout_ms,
            throttle_delay_ms: value.throttle_delay_ms,
            max_payload_size: value.max_payload_size,
            max_inflight_count: value.max_inflight_count,
            max_inflight_size: value.max_inflight_size,
            dynamic_filters: value.dynamic_filters,
            auth: match value.auth {
                Some(m) => Arc::new(StaticAuthenticator { logins: m }),
                None => Arc::new(AllowAuthenticator),
            },
        };
    }
}
