use std::sync::Arc;

use crate::{
    consolelink::{self, ConsoleLink},
    Config, Id, Server,
};
use futures_util::stream::{FuturesUnordered, StreamExt};
use mqttbytes::{Packet, Publish, QoS, RetainForwardRule, Subscribe, SubscribeFilter};
use rumqttlog::{
    Connection, ConnectionAck, Data, Event, Notification, Receiver, RecvError, Router, SendError,
    Sender,
};

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("Unexpected router message")]
    NotConnectionAck(Notification),
    #[error("Connack error {0}")]
    ConnectionAck(String),
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
}

pub struct AsyncLinkTx {
    id: Id,
    router_tx: Sender<(Id, Event)>,
}

impl AsyncLinkTx {
    /// Sends a MQTT Publish to the router
    pub async fn publish<S, V>(
        &mut self,
        topic: S,
        retain: bool,
        payload: V,
    ) -> Result<(), LinkError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, QoS::AtMostOnce, payload);
        publish.retain = retain;
        let message = Event::Data(vec![Packet::Publish(publish)]);
        self.router_tx.async_send((self.id, message)).await?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub async fn subscribe<S: Into<String>>(
        &mut self,
        filters: impl IntoIterator<Item = S>,
    ) -> Result<(), LinkError> {
        let filters = filters
            .into_iter()
            .map(|topic_path| SubscribeFilter {
                path: topic_path.into(),
                qos: QoS::AtMostOnce,
                nolocal: false,
                preserve_retain: false,
                retain_forward_rule: RetainForwardRule::OnEverySubscribe,
            })
            .collect();
        let subscribe = Subscribe { pkid: 0, filters };
        let packet = Packet::Subscribe(subscribe);
        let message = Event::Data(vec![packet]);
        self.router_tx.async_send((self.id, message)).await?;
        Ok(())
    }
}

pub struct AsyncLinkRx {
    id: Id,
    router_tx: Sender<(Id, Event)>,
    link_rx: Receiver<Notification>,
}

impl AsyncLinkRx {
    pub async fn recv(&mut self) -> Result<Data, LinkError> {
        loop {
            let message = self.link_rx.async_recv().await?;
            if let Some(message) = self.handle_router_response(message).await? {
                return Ok(message);
            }
        }
    }

    async fn handle_router_response(
        &mut self,
        message: Notification,
    ) -> Result<Option<Data>, LinkError> {
        match message {
            Notification::ConnectionAck(_) => unreachable!("ConnAck handled in connect"),
            Notification::Message(_) => unreachable!("Local links are always clean"),
            Notification::Data(reply) => {
                trace!(
                    "{:11} {:14} Id = {}, Count = {}",
                    "data",
                    "reply",
                    self.id,
                    reply.payload.len()
                );

                Ok(Some(reply))
            }
            Notification::Pause => {
                let message = (self.id, Event::Ready);
                self.router_tx.async_send(message).await?;
                Ok(None)
            }
            notification => {
                warn!("{:?} not supported in local link", notification);
                Ok(None)
            }
        }
    }
}

#[derive(Clone)]
pub struct LinkBuilder {
    router_tx: Sender<(Id, Event)>,
}

impl LinkBuilder {
    pub async fn connect(
        self,
        client_id: &str,
        max_inflight_requests: usize,
    ) -> Result<(AsyncLinkTx, AsyncLinkRx), LinkError> {
        // connection queue capacity should match maximum inflight requests
        let (connection, link_rx) = Connection::new_remote(client_id, true, max_inflight_requests);

        self.router_tx
            .async_send((0, Event::Connect(connection)))
            .await?;

        // Right now link identifies failure with dropped rx in router, which is probably ok
        // We need this here to get id assigned by router
        let id = match link_rx.async_recv().await? {
            Notification::ConnectionAck(ConnectionAck::Success((id, _, _))) => Ok(id),
            Notification::ConnectionAck(ConnectionAck::Failure(reason)) => {
                Err(LinkError::ConnectionAck(reason))
            }
            other => Err(LinkError::NotConnectionAck(other)),
        }?;

        // Send initialization requests from tracker [topics request and acks request]
        let rx = AsyncLinkRx {
            id,
            router_tx: self.router_tx.clone(),
            link_rx,
        };
        let tx = AsyncLinkTx {
            id,
            router_tx: self.router_tx,
        };

        Ok((tx, rx))
    }
}

pub fn construct_broker(
    config: Config,
) -> (
    Router,
    impl FnOnce(),
    impl std::future::Future<Output = ()>,
    LinkBuilder,
) {
    let (router, router_tx) = Router::new(Arc::new(config.router.clone()));

    let console = {
        let config = config.clone().into();
        let router_tx = router_tx.clone();
        // `ConsoleLink::new` won't terminate until router is running
        || consolelink::start(ConsoleLink::new(config, router_tx).into())
    };

    let server = config
        .servers
        .into_iter()
        .map(|(id, config)| {
            let router_tx = router_tx.clone();
            async {
                if let Err(e) = Server::new(id, config, router_tx).start().await {
                    error!("Accept loop error: {:?}", e);
                }
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<()>();

    let builder = LinkBuilder { router_tx };

    (router, console, server, builder)
}
