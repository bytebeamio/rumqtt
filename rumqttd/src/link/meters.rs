use crate::router::{Event, GetMeter, Meter};
use crate::ConnectionId;
use flume::{Receiver, RecvError, RecvTimeoutError, SendError, Sender, TrySendError};

#[derive(Debug, thiserror::Error)]
pub enum MetersError {
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
    #[error("Meterslink is already in use")]
    Unavailable,
}

pub struct MetersLink {
    router_tx: Sender<(ConnectionId, Event)>,
    router_rx: Receiver<Vec<Meter>>,
}

impl MetersLink {
    pub fn new(router_tx: Sender<(ConnectionId, Event)>) -> Result<MetersLink, MetersError> {
        let (tx, rx) = flume::bounded(5);
        router_tx.send((0, Event::NewMeter(tx)))?;
        let _meter = rx.recv().map_err(|_| MetersError::Unavailable)?;

        let link = MetersLink {
            router_tx,
            router_rx: rx,
        };

        Ok(link)
    }

    pub async fn init(router_tx: Sender<(ConnectionId, Event)>) -> Result<MetersLink, MetersError> {
        let (tx, rx) = flume::bounded(5);
        router_tx.send((0, Event::NewMeter(tx)))?;
        let _meter = rx.recv_async().await?;

        let link = MetersLink {
            router_tx,
            router_rx: rx,
        };

        Ok(link)
    }

    pub fn get(&self, meter: GetMeter) -> Result<Vec<Meter>, MetersError> {
        self.router_tx.send((0, Event::GetMeter(meter)))?;
        let meter = self.router_rx.recv()?;
        Ok(meter)
    }

    pub async fn fetch(&self, meter: GetMeter) -> Result<Vec<Meter>, MetersError> {
        self.router_tx
            .send_async((0, Event::GetMeter(meter)))
            .await?;
        let meter = self.router_rx.recv_async().await?;
        Ok(meter)
    }
}
