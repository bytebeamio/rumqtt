use thiserror::Error;

use std::io;

use crate::tracker::Tracker;
use async_channel::{Sender, Receiver, SendError, RecvError, bounded};
use crate::mesh::ConnectionId;
use crate::{RouterInMessage, Connection, RouterOutMessage};
use crate::router::ConnectionType;
use rumqttc::{Connect, Network, MqttState};
use tokio::net::TcpStream;
use tokio::{time, select};
use std::time::Duration;

#[derive(Error, Debug)]
#[error("...")]
pub enum LinkError {
    Io(#[from] io::Error),
    Send(#[from] SendError<(usize, RouterInMessage)>),
    Recv(#[from] RecvError),
}

/// A link is a connection to another router
pub struct Replicator {
    /// Id of the link. Id of the replica node that this connection is linked with
    remote_id: usize,
    /// Id of this router node. MQTT connection is made with this ID
    local_id: usize,
    /// Tracks the offsets and status of all the topic offsets
    tracker: Tracker,
    /// Handle to send data to router
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
    /// Handle to this link which router uses
    link_rx: Receiver<RouterOutMessage>,
    /// Connections receiver in server mode
    connections_rx: Receiver<Network>,
    /// State of this connection
    state: MqttState,
    /// Remote address to connect to. Used in client mode
    remote: String,
    /// Max inflight
    max_inflight: u16,
}

impl Replicator {
    /// New mesh link. This task is always alive unlike a connection task event though the connection
    /// might have been down. When the connection is broken, this task informs supervisor about it
    /// which establishes a new connection on behalf of the link and forwards the connection to this
    /// task. If this link is a server, it waits for the other end to initiate the connection
    pub async fn new(
        local_id: usize,
        remote_id: usize,
        router_tx: Sender<(ConnectionId, RouterInMessage)>,
        connections_rx: Receiver<Network>,
        remote: String
    ) -> Replicator {
        // Register this link with router even though there is no network connection with other router yet.
        // Actual connection will be requested in `start`
        info!("Initializing link {} <-> {}. Remote = {:?}", local_id, remote_id, remote);
        let max_inflight = 100;

        // Subscribe to all the data as we want to replicate everything.
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");
        let link_rx = register_with_router(remote_id, &router_tx).await;

        Replicator {
            remote_id,
            local_id,
            tracker,
            router_tx,
            connections_rx,
            link_rx,
            state: MqttState::new(max_inflight),
            remote,
            max_inflight,
        }
    }


    /// Inform the supervisor for new connection if this is a client link. Wait for
    /// a new connection handle if this is a server link
    async fn connect(&mut self) -> Network {
        if !self.remote.is_empty() {
            loop {
                let stream = match TcpStream::connect(&self.remote).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to connect to node {}. Error = {:?}", self.remote, e);
                        time::delay_for(Duration::from_secs(1)).await;
                        continue
                    }
                };

                let mut network = Network::new(stream);
                let connect = Connect::new(self.local_id.to_string());
                if let Err(e) = network.connect(connect).await {
                    error!("Failed to mqtt connect to node {}. Error = {:?}", self.remote, e);
                    time::delay_for(Duration::from_secs(1)).await;
                    continue
                }

                if let Err(e) = network.read_connack().await {
                    error!("Failed to read connack from node = {}. Error = {:?}", self.remote, e);
                    time::delay_for(Duration::from_secs(1)).await;
                    continue
                }

                return network
            }
        }

        let network = self.connections_rx.recv().await.unwrap();
        network
    }

    async fn select(&mut self) -> Result<(), LinkError> {
        let mut network = self.connect().await;
        let tracker = &mut self.tracker;

        loop {
            let inflight_full = self.state.inflight() >= self.max_inflight;
            // dbg!(inflight_full, tracker.has_next());
            select! {
                // Pull a bunch of packets from network, reply in bunch and yield the first item
                o = network.readb() => {
                    let incoming = o?;
                    debug!("Incoming packets = {:?}", incoming);
                },
                Some(message) = tracker_next(tracker), if !inflight_full && tracker.has_next() => {
                    debug!("Tracker next = {:?}", message);
                    self.router_tx.send((self.remote_id, message)).await?;
                }
                response = self.link_rx.recv() => {
                    let response = response?;
                    debug!("Router response = {:?}", response);
                },
            }
        }
    }

    pub async fn start(&mut self) {
        loop {
            if let Err(e) = self.select().await {
                error!("Link failed. Error = {:?}", e);
            }
        }
    }
}

async fn register_with_router(id: usize, tx: &Sender<(usize, RouterInMessage)>) -> Receiver<RouterOutMessage> {
    let (link_tx, link_rx) = bounded(4);
    let connection = Connection {
        conn: ConnectionType::Replicator(id),
        handle: link_tx,
    };

    let message = RouterInMessage::Connect(connection);
    tx.send((id, message)).await.unwrap();
    link_rx
}

async fn tracker_next(tracker: &mut Tracker) -> Option<RouterInMessage> {
    tracker.next()
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
