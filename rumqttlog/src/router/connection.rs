use crate::Notification;
use jackiechan::{bounded, Receiver, Sender, TrySendError};
use mqtt4bytes::LastWill;
use std::fmt;

#[derive(Debug, Clone)]
pub enum ConnectionType {
    Device(String),
    Replicator(usize),
}

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
pub struct Connection {
    /// Kind of connection. A replicator connection or a device connection
    /// Replicator connection are only created from inside this library.
    /// All the external connections are of 'device' type
    pub conn: ConnectionType,
    /// Clean session
    clean: bool,
    /// Connection will
    will: Option<LastWill>,
    /// Last failed message to connection
    last_failed: Option<Notification>,
    /// Handle which is given to router to allow router to comminicate with
    /// this connection
    handle: Sender<Notification>,
    /// Capacity of the channel
    capacity: usize,
    /// Last seen remaining space in the channel
    remaining_space: usize,
}

impl Connection {
    pub fn new_remote(
        id: &str,
        clean: bool,
        capacity: usize,
    ) -> (Connection, Receiver<Notification>) {
        let (this_tx, this_rx) = bounded(capacity);

        let connection = Connection {
            conn: ConnectionType::Device(id.to_owned()),
            clean,
            will: None,
            last_failed: None,
            handle: this_tx,
            capacity,
            remaining_space: capacity,
        };

        (connection, this_rx)
    }

    pub fn new_replica(
        id: usize,
        clean: bool,
        capacity: usize,
    ) -> (Connection, Receiver<Notification>) {
        let (this_tx, this_rx) = bounded(capacity);

        let connection = Connection {
            conn: ConnectionType::Replicator(id),
            clean,
            will: None,
            last_failed: None,
            handle: this_tx,
            capacity,
            remaining_space: capacity,
        };

        (connection, this_rx)
    }

    pub fn clean(&self) -> bool {
        self.clean
    }

    pub fn will(&mut self) -> Option<LastWill> {
        self.will.take()
    }

    pub fn set_will(&mut self, will: LastWill) {
        self.will = Some(will);
    }

    /// Sends notification and returns status to unschedule this connection
    pub fn notify(&mut self, notification: Notification) -> bool {
        if let Err(e) = self.handle.try_send(notification) {
            match e {
                TrySendError::Full(_e) => unreachable!(),
                TrySendError::Closed(e) => {
                    self.last_failed = Some(e);
                    return true;
                }
            }
        }

        self.remaining_space -= 1;

        // Update remaining space if there is room for only one notification.
        if self.remaining_space <= 1 {
            self.remaining_space = self.capacity - self.handle.len();

            // If remaining space is still 1 after refresh, send pause notification
            // to the connection and return unschedule true
            if self.remaining_space <= 1 {
                let notification = Notification::Pause;
                if let Err(e) = self.handle.try_send(notification) {
                    match e {
                        TrySendError::Full(_) => unreachable!(),
                        TrySendError::Closed(e) => self.last_failed = Some(e),
                    }
                }

                return true;
            }
        }

        false
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.conn)
    }
}
