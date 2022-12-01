use crate::router::{Event, Subscription};
use crate::ConnectionId;
use flume::{Receiver, RecvError, SendError, Sender};

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
}

pub struct SubscriptionsLink {
    router_rx: Receiver<(ConnectionId, Subscription)>,
}

impl SubscriptionsLink {
    pub fn new(router_tx: Sender<(ConnectionId, Event)>) -> Result<SubscriptionsLink, LinkError> {
        let (link_tx, router_rx) = flume::bounded(5);
        router_tx.send((0, Event::NewSubscription(link_tx)))?;

        assert!(matches!(router_rx.recv()?, (_, Subscription::Created)));

        let link = SubscriptionsLink {
            router_rx,
        };

        Ok(link)
    }

    pub async fn init(router_tx: Sender<(ConnectionId, Event)>) -> Result<SubscriptionsLink, LinkError> {
        let (link_tx, router_rx) = flume::bounded(5);
        router_tx.send((0, Event::NewSubscription(link_tx)))?;

        assert!(matches!(router_rx.recv_async().await?, (_, Subscription::Created)));

        let link = SubscriptionsLink {
            router_rx,
        };

        Ok(link)
    }

    pub fn recv(&self) -> Result<Subscription, LinkError> {
        let (_, subscription) = self.router_rx.recv()?;

        Ok(subscription)
    }

    pub async fn recv_async(&self) -> Result<Subscription, LinkError> {
        let (_, subscription) = self.router_rx.recv_async().await?;

        Ok(subscription)
    }
}
