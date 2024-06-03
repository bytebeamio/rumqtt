use std::collections::{HashMap, HashSet, VecDeque};

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
        self.connections.remove(id)
    }

    /// Save connection tracker
    pub fn save_state(
        &mut self,
        mut tracker: Tracker,
        subscriptions: HashSet<String>,
        metrics: ConnectionEvents,
        unacked_pubrels: VecDeque<u16>,
    ) {
        tracker.pause(PauseReason::Busy);
        let id = tracker.id.clone();

        let session_state = SessionState {
            tracker,
            subscriptions,
            unacked_pubrels,
        };

        self.connections.insert(
            id,
            SavedState {
                session_state: Some(session_state),
                metrics,
            },
        );
    }

    /// Save only metrics for connection
    pub fn save_metrics(&mut self, id: String, metrics: ConnectionEvents) {
        self.connections.insert(
            id,
            SavedState {
                session_state: None,
                metrics,
            },
        );
    }
}

#[derive(Debug)]
pub struct SavedState {
    pub session_state: Option<SessionState>,
    pub metrics: ConnectionEvents,
}

#[derive(Debug)]
pub struct SessionState {
    pub tracker: Tracker,
    pub subscriptions: HashSet<String>,
    // used for pubrel in qos2
    pub unacked_pubrels: VecDeque<u16>,
}
