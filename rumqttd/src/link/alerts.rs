use std::collections::VecDeque;

use crate::router::{Alert, Event};
use crate::{ConnectionId, Filter};
use flume::{Receiver, RecvError, RecvTimeoutError, SendError, Sender, TrySendError};

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel timeout recv error")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("Timeout = {0}")]
    Elapsed(#[from] tokio::time::error::Elapsed),
}

pub struct AlertsLink {
    _alert_id: ConnectionId,
    alert_buffer: VecDeque<(ConnectionId, Alert)>,
    pub router_rx: Receiver<(ConnectionId, Alert)>,
}

impl AlertsLink {
    pub fn new(
        router_tx: Sender<(ConnectionId, Event)>,
        filters: Vec<Filter>,
    ) -> Result<AlertsLink, LinkError> {
        let (tx, rx) = flume::bounded(100);
        router_tx.send((0, Event::NewAlert(tx, filters)))?;
        let (_alert_id, _meter) = rx.recv()?;

        let link = AlertsLink {
            _alert_id,
            alert_buffer: VecDeque::new(),
            router_rx: rx,
        };

        Ok(link)
    }

    pub fn poll(&mut self) -> (ConnectionId, Alert) {
        self.alert_buffer.extend(self.router_rx.drain());

        match self.alert_buffer.pop_front() {
            Some(a) => a,
            None => self.router_rx.recv().unwrap(),
        }
    }
}
