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
}

impl Graveyard {
    pub fn new() -> Graveyard {
        Graveyard {
            connections: HashMap::new(),
        }
    }

    /// Add a new connection.
    /// Return tracker of previous connection if connection id already exists
    pub fn retrieve(&mut self, id: &str) -> Option<SavedState> {
        self.cleanup_expired_sessions();
        self.connections.remove(id)
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
        metrics: ConnectionEvents,
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
                metrics,
                unacked_pubrels,
                expiry: expiry_interval,
            },
        );
    }
}

#[derive(Debug)]
pub struct SavedState {
    pub tracker: Tracker,
    pub subscriptions: HashSet<String>,
    pub metrics: ConnectionEvents,
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
            metrics: ConnectionEvents::default(),
            unacked_pubrels: VecDeque::new(),
            expiry: None,
        }
    }
}
