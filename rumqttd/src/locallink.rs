use rumqttlog::{Sender, Receiver, RouterInMessage, RouterOutMessage, tracker::Tracker, SendError, RecvError, ConnectionAck, Connection, DataReply};
use crate::Id;
use tokio::select;

const MAX_INFLIGHT_REQUESTS: usize = 100;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unexpected router message")]
    RouterMessage(RouterOutMessage),
    #[error("Connack error {0}")]
    ConnAck(String),
    #[error("Channel send error")]
    Send(#[from] SendError<(Id, RouterInMessage)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
}

pub struct LocalLink {
    id: Id,
    tracker: Tracker,
    router_tx: Sender<(Id, RouterInMessage)>,
    link_rx: Receiver<RouterOutMessage>,
}

impl LocalLink {
    pub async fn new(client_id: &str, capacity: usize, router_tx: Sender<(Id, RouterInMessage)>) -> Result<(Id, LocalLink), Error> {
        // Register this connection with the router. Router replies with ack which if ok will
        // start the link. Router can sometimes reject the connection (ex max connection limit)
        let (connection, link_rx) = Connection::new(client_id, capacity);
        let message = (0, RouterInMessage::Connect(connection));
        router_tx.send(message).await.unwrap();

        // Right now link identifies failure with dropped rx in router, which is probably ok for now
        let id = match link_rx.recv().await? {
            RouterOutMessage::ConnectionAck(ack) => match ack {
                ConnectionAck::Success(id) => id,
                ConnectionAck::Failure(reason) => return Err(Error::ConnAck(reason))
            }
            message => return Err(Error::RouterMessage(message))
        };

        Ok((id, LocalLink {
            id,
            tracker: Tracker::new(100),
            router_tx,
            link_rx,
        }))
    }

    pub async fn poll(&mut self) -> Result<Option<DataReply>, Error> {
        select! {
            message = self.link_rx.recv() => {
                let message = message?;
                return Ok(self.handle_router_response(message))
            },
            Some(message) = tracker_next(&mut self.tracker),
            if self.tracker.has_next() && self.tracker.inflight() < MAX_INFLIGHT_REQUESTS => {
                trace!("{:11} {:14} Id = {}, Message = {:?}", "tacker", "next", self.id, message);
                self.router_tx.send((self.id, message)).await?;
                return Ok(None)
            }
        }
    }

    fn handle_router_response(&mut self, message: RouterOutMessage) -> Option<DataReply> {
        match message {
            RouterOutMessage::TopicsReply(reply) => {
                trace!("{:11} {:14} Id = {}, Count = {}", "topics", "reply", self.id, reply.topics.len());
                self.tracker.track_new_topics(&reply);
                None
            }
            RouterOutMessage::DataReply(reply) => {
                trace!("{:11} {:14} Id = {}, Count = {}", "data", "reply", self.id, reply.payload.len());
                self.tracker.update_data_tracker(&reply);
                Some(reply)
            }
            RouterOutMessage::AllTopicsReply(reply) => {
                trace!("{:11} {:14} Id = {}, Count = {}", "alltopics", "reply", self.id, reply.topics.len());
                self.tracker.track_all_topics(&reply);
                None
            }
            message => {
                warn!("Message = {:?} not supported", message);
                None
            }
        }
    }
}

async fn tracker_next(tracker: &mut Tracker) -> Option<RouterInMessage> {
    tracker.next()
}