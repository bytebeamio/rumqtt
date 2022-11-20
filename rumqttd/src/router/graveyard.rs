use std::collections::{HashMap, HashSet};

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
    pub fn save(
        &mut self,
        mut tracker: Tracker,
        subscriptions: HashSet<String>,
        metrics: ConnectionEvents,
    ) {
        tracker.pause(PauseReason::Busy);
        let id = tracker.id.clone();

        self.connections.insert(
            id,
            SavedState {
                tracker,
                subscriptions,
                metrics,
            },
        );
    }
}

#[derive(Debug)]
pub struct SavedState {
    pub tracker: Tracker,
    pub subscriptions: HashSet<String>,
    pub metrics: ConnectionEvents,
}

impl SavedState {
    pub fn new(client_id: String) -> SavedState {
        SavedState {
            tracker: Tracker::new(client_id),
            subscriptions: HashSet::new(),
            metrics: ConnectionEvents::default(),
        }
    }
}
