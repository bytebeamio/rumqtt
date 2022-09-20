use crate::ConnectionId;
use std::collections::VecDeque;

/// Waiters are connections which are waiting to be notified. They have
/// made a data request but router didn't respond because of the connection
/// being caughtup. Waiters stores connection id and pending request
#[derive(Debug)]
pub struct Waiters<T> {
    /// Waiters on new topics
    current: VecDeque<(ConnectionId, T)>,
}

impl<T> Waiters<T> {
    pub fn with_capacity(max_connections: usize) -> Waiters<T> {
        Waiters {
            current: VecDeque::with_capacity(max_connections),
        }
    }

    /// Current parked connection requests waiting for new data
    pub fn waiters(&self) -> &VecDeque<(ConnectionId, T)> {
        &self.current
    }

    /// Pushes a request to current wait queue
    pub fn register(&mut self, id: ConnectionId, request: T) {
        let request = (id, request);
        self.current.push_back(request);
    }

    /// Swaps next wait queue with current wait queue
    pub fn take(&mut self) -> Option<VecDeque<(ConnectionId, T)>> {
        if self.current.is_empty() {
            return None;
        }

        let next = VecDeque::new();
        Some(std::mem::replace(&mut self.current, next))
    }

    /// Remove a connection from waiters
    pub fn remove(&mut self, id: ConnectionId) -> Vec<T> {
        let mut requests = Vec::new();

        while let Some(index) = self.current.iter().position(|x| x.0 == id) {
            let request = self.current.swap_remove_back(index).map(|v| v.1).unwrap();
            requests.push(request)
        }

        requests
    }

    pub fn get_mut(&mut self) -> &mut VecDeque<(ConnectionId, T)> {
        &mut self.current
    }
}
