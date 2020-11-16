use crate::router::Tracker;
use crate::{ConnectionId, Notification};
use std::collections::HashMap;

struct SavedState {
    id: ConnectionId,
    tracker: Option<Tracker>,
    pending: Option<Vec<Notification>>,
}

pub struct ConnectionsLog {
    connections: HashMap<String, SavedState>,
}

impl ConnectionsLog {
    pub fn new() -> ConnectionsLog {
        ConnectionsLog {
            connections: HashMap::new(),
        }
    }

    pub fn id(&self, id: &str) -> Option<ConnectionId> {
        self.connections.get(id).map(|v| v.id)
    }

    pub fn add(
        &mut self,
        id: &str,
        connection_id: ConnectionId,
    ) -> (Option<Tracker>, Option<Vec<Notification>>) {
        match self.connections.get_mut(id) {
            // Return tracker of previous connection for persistent connection
            Some(savedstate) => {
                savedstate.id = connection_id;
                (savedstate.tracker.take(), savedstate.pending.take())
            }
            // Add new connection if this is the first connection with this id
            None => {
                self.connections.insert(
                    id.to_owned(),
                    SavedState {
                        id: connection_id,
                        tracker: None,
                        pending: None,
                    },
                );

                (None, None)
            }
        }
    }

    pub fn save(&mut self, id: &str, mut tracker: Tracker, pending: Vec<Notification>) {
        tracker.set_busy_unschedule(false);
        tracker.set_empty_unschedule(false);

        if let Some(graveyard) = self.connections.get_mut(id) {
            graveyard.tracker = Some(tracker);
            graveyard.pending = Some(pending);
        }
    }
}
