use tokio::sync::mpsc::{Receiver, Sender};

use rumq_core::Packet;

use std::collections::HashMap;

use crate::graveyard::Graveyard;

pub struct Router {
    // handles to all connections. used to route data
    connections: HashMap<String, Sender<Packet>>,
    // maps subscription to interested clients. wildcards
    // aren't supported
    subscriptions: HashMap<String, Vec<String>>,
    // channel receiver to receive data from all the connections.
    // each connection will have a tx handle
    data_rx: Receiver<Packet>,
}

impl Router {
    pub fn new(graveyard: Graveyard, data_rx: Receiver<Packet>) -> Self {
        Router { connections: HashMap::new(), subscriptions: HashMap::new(), data_rx }
    }
}
