use crate::router::Tracker;
use std::collections::HashMap;

pub struct ConnectionsLog {
    connections: HashMap<String, Option<Tracker>>,
}

impl ConnectionsLog {
    pub fn new() -> ConnectionsLog {
        ConnectionsLog {
            connections: HashMap::new(),
        }
    }

    pub fn add(&mut self, id: &str) -> Option<Tracker> {
        match self.connections.get_mut(id) {
            // Return tracker of previous connection for persistent connection
            Some(tracker) => tracker.take(),
            // Add new connection if this is the first connection with this id
            None => {
                self.connections.insert(id.to_owned(), None);
                None
            }
        }
    }

    pub fn save(&mut self, id: &str, mut tracker: Tracker) {
        tracker.set_busy_unschedule(false);
        tracker.set_empty_unschedule(false);
        self.connections.insert(id.to_owned(), Some(tracker));
    }
}
