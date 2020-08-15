use std::io;

use crate::tracker::Tracker;
use crate::mesh::ConnectionId;
use crate::{RouterInMessage, Connection, RouterOutMessage};
use crate::router::ConnectionType;
use crate::router::ReplicationData;
use crate::mesh::state::{State, StateError};
use crate::router::ReplicationAck;

use async_channel::{Sender, Receiver, SendError, RecvError, bounded};
use rumqttc::{Connect, Incoming, Network, PubAck, Request, Publish, QoS};
use tokio::net::TcpStream;
use tokio::{time, select};
use std::time::Duration;
use bytes::{BytesMut, BufMut, Buf};
use thiserror::Error;

#[derive(Error, Debug)]
#[error("...")]
pub enum LinkError {
    Io(#[from] io::Error),
    Send(#[from] SendError<(usize, RouterInMessage)>),
    Recv(#[from] RecvError),
    State(#[from] StateError)
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
    state: State,
    /// Remote address to connect to. Used in client mode
    remote: String,
    /// Max inflight
    max_inflight: u16,
    network: Network,
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
        // Register this link with router even though there is no network connection
        // with other router yet. Actual connection will be requested in `start`
        info!("Initializing link {} <-> {}. Remote = {:?}", local_id, remote_id, remote);
        let max_inflight = 100;

        // Subscribe to all the data as we want to replicate everything.
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");
        let link_rx = register_with_router(remote_id, &router_tx).await;

        let network = connect(&remote, local_id, &connections_rx).await;
        Replicator {
            remote_id,
            local_id,
            tracker,
            router_tx,
            connections_rx,
            link_rx,
            state: State::new(max_inflight),
            remote,
            max_inflight,
            network
        }
    }

    async fn handle_network_data(&mut self, incoming: Vec<Incoming>) -> Result<(), LinkError> {
        let mut data = Vec::new();
        let mut acks = Vec::new();

        for incoming in incoming {
            match incoming {
                Incoming::Publish(publish) => {
                    self.tracker.track_watermark(&publish.topic);
                    let Publish { pkid, topic, mut payload, .. } = publish;
                    let count = payload.get_u32() as usize;
                    let mut replication_data = ReplicationData::with_capacity(pkid, topic, count);
                    for _i in 0..count {
                        let len = payload.get_u32() as usize;
                        let packet = payload.split_to(len);
                        replication_data.push(packet) ;
                    }

                    data.push( replication_data);
                }
                Incoming::PingReq => {
                    self.network.fill2(Request::PingResp)?;
                }
                Incoming::PingResp => {
                    self.state.handle_incoming_pingresp()?;
                }
                Incoming::PubAck(puback) => {
                    let (topic, offset) = self.state.handle_incoming_puback(puback)?;
                    let ack = ReplicationAck::new(topic, offset);
                    acks.push(ack);
                }
                packet => {
                    error!("Packet = {:?} not supported yet", packet);
                }
            }
        }

        if !data.is_empty() {
            let message = RouterInMessage::ReplicationData(data);
            self.router_tx.send((self.remote_id, message)).await?;
        }

        if !acks.is_empty() {
            let message = RouterInMessage::ReplicationAcks(acks);
            self.router_tx.send((self.remote_id, message)).await?;
        }
        Ok(())
    }


    async fn handle_router_response(&mut self, message: RouterOutMessage) -> Result<(), LinkError> {
        match message {
            RouterOutMessage::TopicsReply(reply) => {
                self.tracker.track_new_topics(&reply)
            }
            RouterOutMessage::ConnectionAck(_) => {}
            RouterOutMessage::DataReply(reply) => {
                self.tracker.update_data_tracker(&reply);
                // merge multiple payloads into 1 publish
                // TODO: Vectorize this
                let payload_size = reply.size;
                let payload_count = reply.payload.len();
                let offset = reply.cursors[self.local_id].1;
                let mut payload = BytesMut::with_capacity(payload_size + 4 * payload_count);
                payload.put_u32(reply.payload.len() as u32);
                // length encode vec<bytes>
                for p in reply.payload {
                    payload.put_u32(p.len() as u32);
                    payload.put_slice(&p[..]);
                }


                let publish = Publish::from_bytes(&reply.topic, QoS::AtLeastOnce, payload.freeze());
                let publish = self.state.handle_outgoing_publish(offset, publish)?;
                self.network.fill2(publish)?;
            }

            RouterOutMessage::AcksReply(reply) => {
                self.tracker.update_watermarks_tracker(&reply);
                for ack in reply.pkids {
                    let ack = PubAck::new(ack);
                    let ack = Request::PubAck(ack);
                    self.network.fill2(ack)?;
                }
            }
            RouterOutMessage::AllTopicsReply(_) => {}
        }

        // FIXME Early returns above will prevent router send and network write
        self.network.flush().await?;
        Ok(())
    }

    async fn select(&mut self) -> Result<(), LinkError> {
        loop {
            let inflight_full = self.state.inflight() >= self.max_inflight;
            select! {
                // Pull a bunch of packets from network, reply in bunch and yield the first item
                o = self.network.readb() => {
                    let incoming = o?;
                    debug!("Incoming packets = {:?}", incoming);
                    self.handle_network_data(incoming).await?;
                },
                Some(message) = tracker_next(&mut self.tracker), if !inflight_full && self.tracker.has_next() => {
                    debug!("Tracker next = {:?}", message);
                    self.router_tx.send((self.remote_id, message)).await?;
                }
                response = self.link_rx.recv() => {
                    let response = response?;
                    debug!("Router response = {:?}", response);
                    self.handle_router_response(response).await?;
                },
            }
        }
    }

    pub async fn start(&mut self) {
        loop {
            if let Err(e) = self.select().await {
                error!("Link failed. Error = {:?}", e);
            }

            let network = connect(&self.remote, self.local_id, &self.connections_rx).await;
            self.network = network;
        }
    }
}

/// Inform the supervisor for new connection if this is a client link. Wait for
    /// a new connection handle if this is a server link
async fn connect(addr: &str, local_id: usize, connections_rx: &Receiver<Network>) -> Network {
    if !addr.is_empty() {
        loop {
            let stream = match TcpStream::connect(addr).await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to connect to node {}. Error = {:?}", addr, e);
                    time::delay_for(Duration::from_secs(1)).await;
                    continue
                }
            };

            let mut network = Network::new(stream, 10 * 1024);
            let connect = Connect::new(local_id.to_string());
            if let Err(e) = network.connect(connect).await {
                error!("Failed to mqtt connect to node {}. Error = {:?}", addr, e);
                time::delay_for(Duration::from_secs(1)).await;
                continue
            }

            if let Err(e) = network.read_connack().await {
                error!("Failed to read connack from node = {}. Error = {:?}", addr, e);
                time::delay_for(Duration::from_secs(1)).await;
                continue
            }

            return network
        }
    }

    let network = connections_rx.recv().await.unwrap();
    network
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
