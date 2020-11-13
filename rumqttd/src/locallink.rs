use crate::Id;
use mqtt4bytes::{Packet, Publish, QoS, Subscribe};
use rumqttlog::{
    Connection, ConnectionAck, Data, Event, Notification, Receiver, RecvError, SendError, Sender,
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

pub struct LinkTx {
    id: usize,
    router_tx: Sender<(Id, Event)>,
    client_id: String,
}

impl LinkTx {
    pub(crate) fn new(client_id: &str, router_tx: Sender<(Id, Event)>) -> LinkTx {
        LinkTx {
            id: 0,
            router_tx,
            client_id: client_id.to_owned(),
        }
    }

    pub fn connect(&mut self, max_inflight_requests: usize) -> Result<LinkRx, LinkError> {
        // connection queue capacity should match that maximum inflight requests
        let (connection, link_rx) =
            Connection::new_remote(&self.client_id, true, max_inflight_requests);

        let message = (0, Event::Connect(connection));
        self.router_tx.send(message).unwrap();

        // Right now link identifies failure with dropped rx in router, which is probably ok
        // We need this here to get id assigned by router
        match link_rx.recv()? {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success((id, _, _)) => self.id = id,
                ConnectionAck::Failure(reason) => return Err(LinkError::ConnectionAck(reason)),
            },
            message => return Err(LinkError::NotConnectionAck(message)),
        };

        // Send initialization requests from tracker [topics request and acks request]
        let rx = LinkRx::new(self.id, self.router_tx.clone(), link_rx);

        Ok(rx)
    }

    /// Sends a MQTT Publish to the router
    pub fn publish<S, V>(&mut self, topic: S, retain: bool, payload: V) -> Result<(), LinkError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.retain = retain;
        let message = Event::Data(vec![Packet::Publish(publish)]);
        self.router_tx.send((self.id, message))?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, filter: S) -> Result<(), LinkError> {
        let subscribe = Subscribe::new(filter.into(), QoS::AtMostOnce);
        let packet = Packet::Subscribe(subscribe);
        let message = Event::Data(vec![packet]);
        self.router_tx.send((self.id, message))?;
        Ok(())
    }
}

pub struct LinkRx {
    id: usize,
    router_tx: Sender<(Id, Event)>,
    link_rx: Receiver<Notification>,
}

impl LinkRx {
    pub(crate) fn new(
        id: usize,
        router_tx: Sender<(Id, Event)>,
        link_rx: Receiver<Notification>,
    ) -> LinkRx {
        LinkRx {
            id,
            router_tx,
            link_rx,
        }
    }

    pub fn recv(&mut self) -> Result<Option<Data>, LinkError> {
        let message = self.link_rx.recv()?;
        let message = self.handle_router_response(message)?;
        Ok(message)
    }

    pub async fn async_recv(&mut self) -> Result<Option<Data>, LinkError> {
        let message = self.link_rx.recv()?;
        let message = self.handle_router_response(message)?;
        Ok(message)
    }

    fn handle_router_response(&mut self, message: Notification) -> Result<Option<Data>, LinkError> {
        match message {
            Notification::ConnectionAck(_) => Ok(None),
            Notification::Message(_) => {
                unreachable!("Local links are always clean");
            }
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
                self.router_tx.send(message)?;
                Ok(None)
            }
            notification => {
                warn!("{:?} not supported in local link", notification);
                Ok(None)
            }
        }
    }
}
