use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::state::MqttState;

use rumq_core::Packet;
use tokio::sync::mpsc::Sender;

/// This is the place where all disconnection state is reaped.
/// For persistent connections,used to pick state back during reconnections.
/// Router uses this to pickup handle of a new connection
/// (instead of connection sending its handle to router during connection)
#[derive(Debug, Clone)]
pub struct Graveyard {
    // handles to a connections. map of client id and connection channel tx
    connection_handles: Arc<Mutex<HashMap<String, Sender<Packet>>>>,
    // state of disconnected persistent clients
    connection_state:   Arc<Mutex<HashMap<String, MqttState>>>,
}

impl Graveyard {
    pub fn new() -> Graveyard {
        Graveyard {
            connection_handles: Arc::new(Mutex::new(HashMap::new())),
            connection_state:   Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn reap(&self, id: &str, state: MqttState) {
        let mut handles = self.connection_state.lock().unwrap();
        handles.insert(id.to_owned(), state);
    }
}
