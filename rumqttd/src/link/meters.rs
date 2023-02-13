use crate::router::{Event, Meter};
use crate::ConnectionId;
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

pub struct MetersLink {
    pub(crate) meter_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    router_rx: Receiver<(ConnectionId, Vec<Meter>)>,
}

impl MetersLink {
    pub fn new(router_tx: Sender<(ConnectionId, Event)>) -> Result<MetersLink, LinkError> {
        let (tx, rx) = flume::bounded(5);
        router_tx.send((0, Event::NewMeter(tx)))?;
        let (meter_id, _meter) = rx.recv()?;

        let link = MetersLink {
            meter_id,
            router_tx,
            router_rx: rx,
        };

        Ok(link)
    }

    pub async fn init(router_tx: Sender<(ConnectionId, Event)>) -> Result<MetersLink, LinkError> {
        let (tx, rx) = flume::bounded(5);
        router_tx.send((0, Event::NewMeter(tx)))?;
        let (meter_id, _meter) = rx.recv_async().await?;

        let link = MetersLink {
            meter_id,
            router_tx,
            router_rx: rx,
        };

        Ok(link)
    }

    pub fn recv(&self) -> Result<(ConnectionId, Vec<Meter>), LinkError> {
        let o = self.router_rx.recv()?;
        Ok(o)
    }

    pub async fn next(&self) -> Result<(ConnectionId, Vec<Meter>), LinkError> {
        let o = self.router_rx.recv_async().await?;
        Ok(o)
    }
}
