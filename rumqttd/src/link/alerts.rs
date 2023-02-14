use crate::router::{Alert, Event};
use crate::ConnectionId;
use flume::{Receiver, RecvError, RecvTimeoutError, SendError, Sender, TryRecvError, TrySendError};

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
    #[error("Channel try_recv error")]
    TryRecv(#[from] TryRecvError),
}

pub struct AlertsLink {
    pub router_rx: Receiver<Vec<Alert>>,
}

impl AlertsLink {
    pub fn new(router_tx: Sender<(ConnectionId, Event)>) -> Result<AlertsLink, LinkError> {
        let (tx, rx) = flume::bounded(100);
        router_tx.send((0, Event::NewAlert(tx)))?;
        let link = AlertsLink { router_rx: rx };
        Ok(link)
    }

    pub fn recv(&self) -> Result<Vec<Alert>, LinkError> {
        let o = self.router_rx.try_recv()?;
        Ok(o)
    }

    pub async fn next(&self) -> Result<Vec<Alert>, LinkError> {
        let o = self.router_rx.recv_async().await?;
        Ok(o)
    }
}
