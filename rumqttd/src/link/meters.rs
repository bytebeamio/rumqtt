use crate::router::{Event, GetMeter, Meter};
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
    router_rx: Receiver<(ConnectionId, Meter)>,
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

    pub fn get(&self, meter: GetMeter) -> Result<Meter, LinkError> {
        self.router_tx
            .send((self.meter_id, Event::GetMeter(meter)))?;
        let (_meter_id, meter) = self.router_rx.recv()?;
        Ok(meter)
    }

    pub async fn fetch(&self, meter: GetMeter) -> Result<Meter, LinkError> {
        self.router_tx
            .send_async((self.meter_id, Event::GetMeter(meter)))
            .await?;
        let (_meter_id, meter) = self.router_rx.recv_async().await?;
        Ok(meter)
    }
}
