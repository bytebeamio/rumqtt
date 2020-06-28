use thiserror::Error;

use std::io;

use crate::tracker::Tracker;
use async_channel::{Sender, Receiver, SendError, RecvError, bounded};
use crate::mesh::ConnectionId;
use crate::{RouterInMessage, RouterOutMessage, Connection, IO};
use crate::router::ConnectionType;
use tokio_util::codec::Framed;
use rumqttc::{EventLoop, MqttCodec, Request};

#[derive(Error, Debug)]
#[error("...")]
pub enum LinkError {
    Io(#[from] io::Error),
    Send(#[from] SendError<(usize, RouterInMessage)>),
    Recv(#[from] RecvError),
}

/// A link is a connection to another router
pub struct Replicator {
    /// Id of the link. Id of the router this connection is with
    id: u8,
    /// Tracks the offsets and status of all the topic offsets
    tracker: Tracker,
    /// Handle to send data to router
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
    /// Handle to this link which router uses
    link_rx: Option<Receiver<RouterOutMessage>>,
    /// Client or server link
    is_client: bool,
}

impl Replicator {
    /// New mesh link. This task is always alive unlike a connection task event though the connection
    /// might have been down. When the connection is broken, this task informs supervisor about it
    /// which establishes a new connection on behalf of the link and forwards the connection to this
    /// task. If this link is a server, it waits for the other end to initiate the connection
    pub async fn new(id: u8, router_tx: Sender<(ConnectionId, RouterInMessage)>, is_client: bool) -> Replicator {
        // Register this link with router even though there is no network connection with other router yet.
        // Actual connection will be requested in `start`
        info!("Creating link {} with router. Client mode = {}", id, is_client);

        // Subscribe to all the data as we want to replicate everything.
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");
        let mut replicator = Replicator {
            id,
            tracker,
            router_tx,
            link_rx: None,
            is_client,
        };

        let link_rx = replicator.register_with_router().await;
        replicator.link_rx = Some(link_rx);
        replicator
    }

    async fn register_with_router(&self) -> Receiver<RouterOutMessage> {
        let (link_tx, link_rx) = bounded(4);
        let connection = Connection {
            conn: ConnectionType::Replicator(self.id as usize),
            handle: link_tx,
        };
        let message = RouterInMessage::Connect(connection);
        self.router_tx.send((self.id as usize, message)).await.unwrap();
        link_rx
    }

    /// Inform the supervisor for new connection if this is a client link. Wait for
    /// a new connection handle if this is a server link
    async fn await_connection<S: IO>(
        &mut self,
        connections_rx: &Receiver<Framed<S, MqttCodec>>
    ) -> EventLoop<Receiver<Request>> {
        let framed = connections_rx.recv().await.unwrap();
        info!("Link with {} successful!!", self.id);
        todo!()
    }

    async fn start(&self) {

    }
}



#[cfg(test)]
mod test {
    #[test]
    fn start_should_iterate_through_all_the_topics() {}

    #[test]
    fn start_should_refresh_topics_from_router_correctly() {}

    #[test]
    fn link_should_make_a_topics_request_after_every_200_iteratoins() {}

    #[test]
    fn link_should_make_a_topics_request_after_all_topics_are_caught_up() {}

    #[test]
    fn link_should_respond_with_new_topics_for_a_previous_empty_topics_request() {}

    #[test]
    fn link_should_not_make_duplicate_topics_request_while_its_waiting_for_previous_reply() {}

    #[test]
    fn link_should_iterate_through_all_the_active_topics() {}

    #[test]
    fn link_should_ignore_a_caught_up_topic() {}

    #[test]
    fn router_should_respond_for_new_data_on_caught_up_topics() {}
}
