use crate::router::Tracker;
use crate::{ConnectionId, RouterId};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub router_id: RouterId,
    pub total_connections: usize,
    pub total_topics: usize,
    pub total_subscriptions: usize,
}

impl Metrics {
    pub fn new(router_id: RouterId) -> Metrics {
        Metrics {
            router_id,
            total_connections: 0,
            total_topics: 0,
            total_subscriptions: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionMetrics {
    id: String,
    tracker: Option<Tracker>,
}

impl ConnectionMetrics {
    pub fn new(id: String) -> ConnectionMetrics {
        ConnectionMetrics { id, tracker: None }
    }
}
