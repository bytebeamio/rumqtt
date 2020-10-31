use crate::router::Tracker;
use crate::{Config, RouterId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum MetricsRequest {
    Config,
    Router,
    Connection(String),
}

#[derive(Debug, Clone)]
pub enum MetricsReply {
    Config(Arc<Config>),
    Router(RouterMetrics),
    Connection(ConnectionMetrics),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterMetrics {
    pub router_id: RouterId,
    pub total_connections: usize,
    pub total_topics: usize,
    pub total_subscriptions: usize,
}

impl RouterMetrics {
    pub fn new(router_id: RouterId) -> RouterMetrics {
        RouterMetrics {
            router_id,
            total_connections: 0,
            total_topics: 0,
            total_subscriptions: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionMetrics {
    id: String,
    tracker: Option<Tracker>,
}

impl ConnectionMetrics {
    pub fn new(id: String, tracker: Option<Tracker>) -> ConnectionMetrics {
        ConnectionMetrics { id, tracker }
    }
}
