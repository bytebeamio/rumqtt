use crate::router::Tracker;
use crate::Notification;
use std::collections::HashMap;

struct Graveyard {
    tracker: Option<Tracker>,
    pending: Option<Vec<Notification>>,
}

pub struct ConnectionsLog {
    connections: HashMap<String, Graveyard>,
}

impl ConnectionsLog {
    pub fn new() -> ConnectionsLog {
        ConnectionsLog {
            connections: HashMap::new(),
        }
    }

    pub fn add(&mut self, id: &str) -> (Option<Tracker>, Option<Vec<Notification>>) {
        match self.connections.get_mut(id) {
            // Return tracker of previous connection for persistent connection
            Some(graveyard) => (graveyard.tracker.take(), graveyard.pending.take()),
            // Add new connection if this is the first connection with this id
            None => {
                self.connections.insert(
                    id.to_owned(),
                    Graveyard {
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
