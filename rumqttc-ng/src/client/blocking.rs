use std::time::Duration;
use base::messages::QoS;

use crate::{Notification, Token};

pub struct Client {
    id: usize,
}

impl Client {
    pub fn set_token_timeout(&self, timeout: Duration) {
        todo!()
    }

    pub fn subscribe(&self, topic: &str, qos: QoS, ack: Ack) -> Result<Token, Error> {
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

pub enum Ack {
    Auto,
    Manual,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
}