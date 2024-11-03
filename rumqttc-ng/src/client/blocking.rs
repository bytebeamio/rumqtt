use base::messages::{Filter, Publish, QoS, RetainForwardRule, Subscribe};
use std::time::Duration;

use crate::{AckSetting, Event, Notification, Request, Token, Tx};

pub struct Client {
    id: usize,
    tx: Tx<Event, Request>,
}

impl Client {
    pub(crate) fn new(id: usize, tx: Tx<Event, Request>) -> Self {
        Self { id, tx }
    }

    pub fn set_token_timeout(&self, timeout: Duration) {
        todo!()
    }

    pub fn subscribe(&mut self, topic: &str, qos: QoS, ack: AckSetting) -> Result<Token, Error> {
        let subscribe = Subscribe {
            pkid: 0,
            filters: vec![Filter {
                path: topic.to_string(),
                qos,
                nolocal: false,
                preserve_retain: false,
                retain_forward_rule: RetainForwardRule::Never,
            }],
            properties: None,
        };

        let request = Request::Subscribe(subscribe, ack);
        let buffer = &mut self.tx.tx.read;
        buffer.push(request);

        if self.tx.tx.try_forward() {
            self.tx.events_tx.send(Event::ClientData);
        }

        let token = Token::new(self.id);
        Ok(token)
    }

    pub fn publish(
        &mut self,
        topic: &str,
        payload: &str,
        qos: QoS,
        retain: bool,
    ) -> Result<Token, Error> {
        let publish = Publish {
            pkid: 0,
            topic: topic.into(),
            payload: payload.as_bytes().to_vec(),
            properties: None,
            dup: false,
            qos,
            retain,
        };

        let request = Request::Publish(publish);
        let buffer = &mut self.tx.tx.read;
        buffer.push(request);

        if self.tx.tx.try_forward() {
            // self.tx.events_tx.send(Event::Forward);
        }

        let token = Token::new(self.id);
        Ok(token)
    }

    pub fn capture_alerts(&self) {
        todo!()
    }

    pub fn next(&mut self) -> Result<Notification, Error> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {}
