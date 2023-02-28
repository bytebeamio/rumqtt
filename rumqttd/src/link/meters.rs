use crate::router::{Event, Meter};
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

pub struct MetersLink {
    router_rx: Receiver<Vec<Meter>>,
}

impl MetersLink {
    pub fn new(router_tx: Sender<(ConnectionId, Event)>) -> Result<MetersLink, LinkError> {
        let (tx, rx) = flume::bounded(100);

        router_tx.send((0, Event::NewMeter(tx)))?;
        let link = MetersLink { router_rx: rx };
        Ok(link)
    }

    pub async fn init(router_tx: Sender<(ConnectionId, Event)>) -> Result<MetersLink, LinkError> {
        let (tx, rx) = flume::bounded(100);

        router_tx.send_async((0, Event::NewMeter(tx))).await?;
        let link = MetersLink { router_rx: rx };
        Ok(link)
    }

    pub fn recv(&self) -> Result<Vec<Meter>, LinkError> {
        let o = self.router_rx.try_recv()?;
        Ok(o)
    }

    pub async fn next(&self) -> Result<Vec<Meter>, LinkError> {
        let o = self.router_rx.recv_async().await?;
        Ok(o)
    }
}
