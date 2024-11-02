use std::time::Duration;
use base::{messages::{Packet, QoS}, EventsTx, XchgPipeA};

use crate::{Event, Notification, Token};

pub struct Client {
    id: usize,
    events_tx: EventsTx<Event>,
    data_tx: XchgPipeA<Packet>,

}

impl Client {
    pub fn set_token_timeout(&self, timeout: Duration) {
        todo!()
    }

    pub fn subscribe(&self, topic: &str, qos: QoS, ack: AckSetting) -> Result<Token, Error> {
        todo!()
    }

    pub fn publish(&self, topic: &str, payload: &str, qos: QoS, retain: bool) -> Result<Token, Error> {
        todo!()
    }
    
    pub fn capture_alerts(&self) {
        todo!()
    }

    pub fn next(&mut self) -> Result<Notification, Error> {
        todo!()
    }
}

pub enum AckSetting {
    Auto,
    Manual,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
}