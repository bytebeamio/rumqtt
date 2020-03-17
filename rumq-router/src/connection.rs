use rumq_core::mqtt4::{Connect, Packet, QoS};
use tokio::sync::mpsc::Sender;

use std::collections::VecDeque;
use std::fmt;

use crate::state::MqttState;
use crate::RouterMessage;

pub struct Connection {
    pub connect: Connect,
    pub handle: Option<Sender<RouterMessage>>,
}

impl Connection {
    pub fn new(connect: Connect, handle: Sender<RouterMessage>) -> Connection {
        Connection {
            connect,
            handle: Some(handle),
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.connect)
    }
}

#[derive(Debug)]
pub struct ActiveConnection {
    pub state: MqttState,
    pub outgoing: VecDeque<Packet>,
    pub tx: Sender<RouterMessage>,
}

impl ActiveConnection {
    pub fn new(tx: Sender<RouterMessage>, state: MqttState) -> ActiveConnection {
        ActiveConnection {
            state,
            outgoing: VecDeque::new(),
            tx,
        }
    }
}

#[derive(Debug)]
pub struct InactiveConnection {
    pub state: MqttState,
}

impl InactiveConnection {
    pub fn new(state: MqttState) -> InactiveConnection {
        InactiveConnection { state }
    }
}

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub client_id: String,
    pub qos: QoS,
}

impl Subscriber {
    pub fn new(id: &str, qos: QoS) -> Subscriber {
        Subscriber {
            client_id: id.to_owned(),
            qos,
        }
    }
}
