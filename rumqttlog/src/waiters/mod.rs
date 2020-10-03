use crate::router::TopicsRequest;
use crate::DataRequest;
use std::collections::{HashMap, VecDeque};

type Topic = String;
type ConnectionId = usize;

/// Waiter on a topic. These are used to wake connections/replicators
/// which are caught up previously and waiting on new data.
pub struct DataWaiters {
    // Map[topic]List[Connections Ids]
    waiters: HashMap<Topic, VecDeque<(ConnectionId, DataRequest)>>,
}

impl DataWaiters {
    pub fn new() -> DataWaiters {
        DataWaiters {
            waiters: HashMap::new(),
        }
    }

    pub fn get_mut(&mut self, topic: &str) -> Option<&mut VecDeque<(ConnectionId, DataRequest)>> {
        self.waiters.get_mut(topic)
    }

    /// Register data waiter
    pub fn register(&mut self, id: ConnectionId, request: DataRequest) {
        let topic = request.topic.clone();
        let request = (id, request);

        match self.waiters.get_mut(&topic) {
            Some(waiters) => waiters.push_back(request),
            None => {
                let mut waiters = VecDeque::new();
                waiters.push_back(request);
                self.waiters.insert(topic, waiters);
            }
        }
    }

    /// Remove a connection from waiters
    pub fn remove(&mut self, id: ConnectionId) {
        for waiters in self.waiters.values_mut() {
            if let Some(index) = waiters.iter().position(|x| x.0 == id) {
                waiters.swap_remove_back(index);
            }
        }
    }
}

pub struct TopicsWaiters {
    /// Waiters on new topics
    waiters: VecDeque<(ConnectionId, TopicsRequest)>,
}

impl TopicsWaiters {
    pub fn new() -> TopicsWaiters {
        TopicsWaiters {
            waiters: VecDeque::new(),
        }
    }

    pub fn pop_front(&mut self) -> Option<(ConnectionId, TopicsRequest)> {
        self.waiters.pop_front()
    }

    pub fn register(&mut self, id: ConnectionId, request: TopicsRequest) {
        trace!("{:11} {:14} Id = {}", "topics", "register", id);
        let request = (id.to_owned(), request);
        self.waiters.push_back(request);
    }

    /// Remove a connection from waiters
    pub fn remove(&mut self, id: ConnectionId) {
        if let Some(index) = self.waiters.iter().position(|x| x.0 == id) {
            self.waiters.swap_remove_back(index);
        }
    }
}
