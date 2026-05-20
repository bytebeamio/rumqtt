//! Cluster configuration types.
//!
//! [`ClusterConfig`] maps directly to the `[cluster]` section in the TOML
//! config file. [`ClusterConnectionConfig`] is the resolved form with parsed
//! node addresses, produced via `TryFrom<ClusterConfig>`.

use serde::{Deserialize, Serialize};

/// Raw cluster configuration as deserialized from TOML.
///
/// The `nodes` field is a comma-separated string of node addresses
/// (e.g. `"node2:1885, node3:1886"`), parsed into a `Vec<String>`
/// when converting to [`ClusterConnectionConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub node_mode: ClusterNodeMode,
    pub node_port: u16,
    pub nodes: String,
    #[serde(default = "default_max_missed_pings")]
    pub max_missed_pings: u32,
    #[serde(default = "default_health_check_window_secs")]
    pub health_check_window_secs: u64,
    #[serde(default = "default_max_reconnect_attempts")]
    pub max_reconnect_attempts: u32,
}

fn default_max_missed_pings() -> u32 {
    3
}

fn default_health_check_window_secs() -> u64 {
    60
}

fn default_max_reconnect_attempts() -> u32 {
    10
}

/// Role of a node within the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClusterNodeMode {
    #[default]
    Master,
    Slave,
}

/// Resolved cluster connection configuration ready for use by [`super::cluster::Cluster`].
///
/// Produced from [`ClusterConfig`] via `TryFrom`. Contains the parsed list
/// of node addresses and validated fields.
pub struct ClusterConnectionConfig {
    pub node_mode: ClusterNodeMode,
    pub node_port: u16,
    pub nodes: Vec<String>,
    pub max_missed_pings: u32,
    pub health_check_window_secs: u64,
    pub max_reconnect_attempts: u32,
}

impl TryFrom<ClusterConfig> for ClusterConnectionConfig {
    type Error = String;

    fn try_from(config: ClusterConfig) -> Result<Self, Self::Error> {
        let nodes: Vec<String> = config
            .nodes
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if nodes.is_empty() {
            return Err("Cluster config must have at least one node".to_string());
        }

        Ok(ClusterConnectionConfig {
            node_mode: config.node_mode,
            node_port: config.node_port,
            nodes,
            max_missed_pings: config.max_missed_pings,
            health_check_window_secs: config.health_check_window_secs,
            max_reconnect_attempts: config.max_reconnect_attempts,
        })
    }
}
