use crate::Id;
use mqtt4bytes::{Packet, Publish, QoS, Subscribe};
use rumqttlog::{
    tracker::Tracker, Connection, ConnectionAck, DataReply, Receiver, RecvError, RouterInMessage,
    RouterOutMessage, SendError, Sender,
};

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("Unexpected router message")]
    NotConnectionAck(RouterOutMessage),
    #[error("Connack error {0}")]
    ConnectionAck(String),
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, RouterInMessage)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
}

pub struct LinkTx {
    id: usize,
    router_tx: Sender<(Id, RouterInMessage)>,
    client_id: String,
    capacity: usize,
}

impl LinkTx {
    pub(crate) fn new(
        client_id: &str,
        capacity: usize,
        router_tx: Sender<(Id, RouterInMessage)>,
    ) -> LinkTx {
        LinkTx {
            id: 0,
            router_tx,
            client_id: client_id.to_owned(),
            capacity,
        }
    }

    pub fn connect(&mut self) -> Result<LinkRx, LinkError> {
        let (connection, link_rx) = Connection::new_remote(&self.client_id, self.capacity);
        let message = (0, RouterInMessage::Connect(connection));
        self.router_tx.send(message).unwrap();
        // Right now link identifies failure with dropped rx in router, which is probably ok
        // We need this here to get id assigned by router
        match link_rx.recv()? {
            RouterOutMessage::ConnectionAck(ack) => match ack {
                ConnectionAck::Success(id) => self.id = id,
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
        let message = RouterInMessage::Publish(publish);
        self.router_tx.send((self.id, message))?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, filter: S) -> Result<(), LinkError> {
        let subscribe = Subscribe::new(filter.into(), QoS::AtMostOnce);
        let packet = Packet::Subscribe(subscribe);
        let message = RouterInMessage::Data(vec![packet]);
        self.router_tx.send((self.id, message))?;
        Ok(())
    }
}

pub struct LinkRx {
    id: usize,
    router_tx: Sender<(Id, RouterInMessage)>,
    link_rx: Receiver<RouterOutMessage>,
    tracker: Tracker,
}

impl LinkRx {
    pub(crate) fn new(
        id: usize,
        router_tx: Sender<(Id, RouterInMessage)>,
        link_rx: Receiver<RouterOutMessage>,
    ) -> LinkRx {
        LinkRx {
            id,
            router_tx,
            link_rx,
            tracker: Tracker::new(100),
        }
    }

    pub fn recv(&mut self) -> Result<Option<DataReply>, LinkError> {
        let message = self.link_rx.recv()?;
        let message = self.handle_router_response(message);

        // A response from router should trigger new request. Some times
        // requests. For example, topics reply should trigger next topics
        // requsest and data request. Not sending a data request is a
        // potential deadlock until next topics response. So just try_send
        // is a problem.
        //
        // send until inflight requests are full
        while let Some(message) = self.tracker.next() {
            trace!("Id = {:14}, Message = {:?}", self.id, message);
            self.router_tx.send((self.id, message))?;
        }

        Ok(message)
    }

    fn handle_router_response(&mut self, message: RouterOutMessage) -> Option<DataReply> {
        match message {
            RouterOutMessage::TopicsReply(reply) => {
                trace!(
                    "{:11} {:14} Id = {}, Count = {}",
                    "topics",
                    "reply",
                    self.id,
                    reply.topics.len()
                );
                self.tracker.track_new_topics(&reply);
                None
            }
            RouterOutMessage::DataReply(reply) => {
                trace!(
                    "{:11} {:14} Id = {}, Count = {}",
                    "data",
                    "reply",
                    self.id,
                    reply.payload.len()
                );
                self.tracker.update_data_tracker(&reply);
                Some(reply)
            }
            message => {
                warn!("Message = {:?} not supported", message);
                None
            }
        }
    }
}
