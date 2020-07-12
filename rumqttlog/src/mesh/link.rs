use thiserror::Error;

use std::io;

use crate::tracker::Tracker;
use async_channel::{Sender, Receiver, SendError, RecvError, bounded};
use crate::mesh::ConnectionId;
use crate::{RouterInMessage, Connection, IO};
use crate::router::ConnectionType;
use rumqttc::{EventLoop, MqttOptions, Request};

#[derive(Error, Debug)]
#[error("...")]
pub enum LinkError {
    Io(#[from] io::Error),
    Send(#[from] SendError<(usize, RouterInMessage)>),
    Recv(#[from] RecvError),
}

/// A link is a connection to another router
pub struct Replicator<S> {
    /// Id of the link. Id of the router this connection is with
    id: u8,
    /// Tracks the offsets and status of all the topic offsets
    tracker: Tracker,
    /// Handle to send data to router
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
    /// Handle to this link which router uses
    link_rx: Option<Receiver<Request>>,
    /// Connections receiver in server mode
    connections_rx: Receiver<S>,
    /// Client or server link
    is_client: bool,
}

impl<S: IO> Replicator<S> {
    /// New mesh link. This task is always alive unlike a connection task event though the connection
    /// might have been down. When the connection is broken, this task informs supervisor about it
    /// which establishes a new connection on behalf of the link and forwards the connection to this
    /// task. If this link is a server, it waits for the other end to initiate the connection
    pub async fn new(
        id: u8,
        router_tx: Sender<(ConnectionId, RouterInMessage)>,
        connections_rx: Receiver<S>,
        is_client: bool
    ) -> Replicator<S> {
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
            connections_rx,
            link_rx: None,
            is_client,
        };

        let link_rx = replicator.register_with_router().await;
        replicator.link_rx = Some(link_rx);
        replicator
    }

    async fn register_with_router(&self) -> Receiver<Request> {
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
    async fn connect(&mut self, cap: usize) -> EventLoop {
        if self.is_client {
            let options = MqttOptions::new(self.id.to_string(), "localhost", 1883);
            return EventLoop::new(options, cap).await;
        }

        let framed = self.connections_rx.recv().await.unwrap();
        let options = MqttOptions::new(self.id.to_string(), "localhost", 1883);
        let mut eventloop = EventLoop::new(options, cap).await;
        // eventloop.set_network(framed);
        eventloop
    }

    pub async fn start(&self) {
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
