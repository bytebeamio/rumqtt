use crate::ConnectionId;
use std::collections::VecDeque;

pub struct ReadyQueue {
    queue: VecDeque<ConnectionId>,
}

impl ReadyQueue {
    pub fn new() -> ReadyQueue {
        ReadyQueue {
            queue: VecDeque::with_capacity(100),
        }
    }

    pub fn pop_front(&mut self) -> Option<ConnectionId> {
        self.queue.pop_front()
    }

    pub fn push_back(&mut self, id: ConnectionId) {
        self.queue.push_back(id)
    }

    /// Remove a connection from waiters
    pub fn remove(&mut self, id: ConnectionId) {
        if let Some(index) = self.queue.iter().position(|x| *x == id) {
            self.queue.swap_remove_back(index);
        }
    }
}
