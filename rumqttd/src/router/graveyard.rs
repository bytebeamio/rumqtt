use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Instant,
};

use super::{
    scheduler::{PauseReason, Tracker},
    ConnectionEvents,
};

pub struct Graveyard {
    connections: HashMap<String, SavedState>,
    metrics: HashMap<String, ConnectionEvents>,
}

impl Graveyard {
    pub fn new() -> Graveyard {
        Graveyard {
            connections: HashMap::new(),
            metrics: HashMap::new(),
        }
    }

    /// Add a new connection.
    /// Return tracker of previous connection if connection id already exists
    pub fn retrieve(&mut self, id: &str) -> Option<SavedState> {
        self.cleanup_expired_sessions();
        self.connections.remove(id)
    }

    pub fn retrieve_metrics(&mut self, id: &str) -> Option<ConnectionEvents> {
        self.metrics.remove(id)
    }

    fn cleanup_expired_sessions(&mut self) {
        let now = Instant::now();
        self.connections.retain(|_, state| {
            let Some(exp) = state.expiry else {
                return true;
            };
            exp > now
        })
    }

    /// Save connection tracker
    pub fn save(
        &mut self,
        mut tracker: Tracker,
        subscriptions: HashSet<String>,
        unacked_pubrels: VecDeque<u16>,
        expiry_interval: Option<Instant>,
    ) {
        tracker.pause(PauseReason::Busy);
        let id = tracker.id.clone();

        self.connections.insert(
            id,
            SavedState {
                tracker,
                subscriptions,
                unacked_pubrels,
                expiry: expiry_interval,
            },
        );
    }

    pub fn save_metrics(&mut self, id: String, metrics: ConnectionEvents) {
        self.metrics.insert(id, metrics);
    }
}

#[derive(Debug)]
pub struct SavedState {
    pub tracker: Tracker,
    pub subscriptions: HashSet<String>,
    // used for pubrel in qos2
    pub unacked_pubrels: VecDeque<u16>,
    // session expiry interval
    pub expiry: Option<Instant>,
}

impl SavedState {
    pub fn new(client_id: String) -> SavedState {
        SavedState {
            tracker: Tracker::new(client_id),
            subscriptions: HashSet::new(),
            unacked_pubrels: VecDeque::new(),
            expiry: None,
        }
    }
}
