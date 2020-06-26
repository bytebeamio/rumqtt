use futures_util::sink::SinkExt;
use thiserror::Error;
use tokio::select;

use std::io;
use std::time::Duration;

use crate::mesh::codec::{MeshCodec, Packet};
use crate::mesh::ConnectionId;
use crate::router::{ConnectionType, Data, TopicsRequest};
use crate::tracker::Tracker;
use crate::{Connection, RouterInMessage, RouterOutMessage, IO};
use async_channel::{bounded, Receiver, RecvError, SendError, Sender};
use bytes::BytesMut;
use futures_util::StreamExt;
use tokio::time;
use tokio_util::codec::Framed;

#[derive(Error, Debug)]
#[error("...")]
pub enum LinkError {
    Io(#[from] io::Error),
    Send(#[from] SendError<(usize, RouterInMessage)>),
    Recv(#[from] RecvError),
}

macro_rules! try_loop {
    ($expr:expr, $broken:expr, $action:stmt) => {
        match $expr {
            Result::Ok(val) => val,
            Result::Err(err) => {
                $broken = true;
                error!("Error = {:?}", err);
                $action
            }
        }
    };
}

/// A link is a connection to another router
pub struct Link {
    /// Id of the link. Id of the router this connection is with
    id: u8,
    /// Tracks the offsets and status of all the topic offsets
    tracker: Tracker,
    /// Current position in topics log
    topics_offset: TopicsRequest,
    /// Handle to send data to router
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
    /// Handle to this link which router uses
    link_rx: Option<Receiver<RouterOutMessage>>,
    /// Connection handle which supervisor uses to pass new connection handles
    /// Handle to supervisor
    supervisor_tx: Sender<u8>,
    /// Client or server link
    is_client: bool,
}

impl Link {
    /// New mesh link. This task is always alive unlike a connection task event though the connection
    /// might have been down. When the connection is broken, this task informs supervisor about it
    /// which establishes a new connection on behalf of the link and forwards the connection to this
    /// task. If this link is a server, it waits for the other end to initiate the connection
    pub async fn new(
        id: u8,
        mut router_tx: Sender<(ConnectionId, RouterInMessage)>,
        supervisor_tx: Sender<u8>,
        is_client: bool,
    ) -> Link {
        // Register this link with router even though there is no network connection with other router yet.
        // Actual connection will be requested in `start`
        info!(
            "Creating link {} with router. Client mode = {}",
            id, is_client
        );
        let link_rx = register_with_router(id, &mut router_tx).await;
        let topics_offset = TopicsRequest {
            offset: 0,
            count: 100,
        };

        // Subscribe to all the data as we want to replicate everything.
        // TODO this will be taken from config in future
        let mut wild_subscriptions = Vec::new();
        wild_subscriptions.push("#".to_owned());
        Link {
            id,
            tracker: Tracker::new(),
            topics_offset,
            router_tx,
            link_rx: Some(link_rx),
            supervisor_tx,
            is_client,
        }
    }

    /// Request for topics to be polled again
    async fn ask_for_more_topics(&mut self) -> Result<(), LinkError> {
        debug!(
            "Topics request. Offset = {}, Count = {}",
            self.topics_offset.offset, self.topics_offset.count
        );
        let message = RouterInMessage::TopicsRequest(self.topics_offset.clone());
        self.router_tx.send((self.id as usize, message)).await?;
        Ok(())
    }

    /// Inform the supervisor for new connection if this is a client link. Wait for
    /// a new connection handle if this is a server link
    async fn connect<S: IO>(
        &mut self,
        connections_rx: &mut Receiver<Framed<S, MeshCodec>>,
    ) -> Framed<S, MeshCodec> {
        info!("Link with {} broken!!", self.id);
        if self.is_client {
            info!("About to make a new connection ...");
            time::delay_for(Duration::from_secs(2)).await;
            self.supervisor_tx.send(self.id).await.unwrap();
        }

        let framed = connections_rx.next().await.unwrap();
        info!("Link with {} successful!!", self.id);
        framed
    }

    /// Start handling the connection
    /// Links and connections communicate with router with a pull. This allows router to just reply.
    /// This ensured router never fills links/connections channel and take care of error handling
    pub async fn start<S: IO>(
        &mut self,
        mut connections_rx: Receiver<Framed<S, MeshCodec>>,
    ) -> Result<(), LinkError> {
        let (router_tx, link_rx) = self.extract_handles();
        let mut framed = self.connect(&mut connections_rx).await;

        self.ask_for_more_topics().await.unwrap();
        let mut broken = false;
        'start: loop {
            if broken {
                drop(framed);
                framed = self.connect(&mut connections_rx).await;
                broken = false;
            }

            // TODO Handle case when connection has failed current publish batch has to be retransmitted
            select! {
                o = framed.next() => {
                    let o = match o {
                        Some(Ok(o)) => o,
                        Some(Err(e)) => {
                            error!("Stream error = {:?}", e);
                            broken = true;
                            continue;
                        }
                        None => {
                            info!("Stream end!!");
                            broken = true;
                            continue;
                        }
                    };

                    match o {
                        Packet::Data(pkid, topic, payload) => {
                            let ack = Packet::DataAck(pkid);
                            try_loop!(framed.send(ack).await, broken, continue 'start);
                            // This is a dummy in replication context
                            let pkid = 0;
                            let data = RouterInMessage::Data(Data { pkid, topic, payload });
                            router_tx.send((self.id as usize, data)).await?;
                        }
                        Packet::DataAck(ack) => {
                            debug!("Replicated till offset = {:?}", ack);
                            match self.tracker.next() {
                                Some(request) => self.router_tx.send((self.id as usize, request)).await?,
                                None => continue
                            };
                        }
                        packet => warn!("Received unsupported packet = {:?}", packet),
                    }
                }
                o = link_rx.recv() => {
                    match o? {
                        RouterOutMessage::ConnectionAck(ack) => {
                            info!("Connection status = {:?}", ack);
                        }
                        // Response to previous data request. Router also returns empty responses.
                        // We use this to mark this topic as 'caught up' and move it from 'active'
                        // to 'pending'. This ensures that next topics iterator ignores this topic
                        RouterOutMessage::DataReply(reply) => {
                            self.tracker.update_data_request(&reply);
                            let topic = reply.topic;

                            // TODO Inefficient. Find a better way to convert Vec<Bytes> -> Bytes
                            let mut out = BytesMut::new();
                            for payload in reply.payload.into_iter() {
                                out.extend_from_slice(&payload[..]);
                            }

                            if out.len() == 0 { continue }
                            let packet = Packet::Data(reply.native_offset, topic.clone(), out.freeze());
                            try_loop!(framed.send(packet).await, broken, continue 'start);
                        }
                        // Refresh the list of interested topics. Router doesn't reply empty responses.
                        // Router registers this link to notify whenever there are new topics. This
                        // behaviour is contrary to data reply where router returns empty responses
                        RouterOutMessage::TopicsReply(reply) => {
                            debug!("Topics reply. Offset = {}, Count = {}", reply.offset, reply.topics.len());
                            self.tracker.update_topics_request(&reply);

                            // New topics. Use this to make a request to wake up request-reply loop
                            match self.tracker.next() {
                                Some(request) => self.router_tx.send((self.id as usize, request)).await?,
                                None => continue,
                            };

                            // next topic request offset
                            self.topics_offset.offset += reply.offset + 1;
                        }
                        message => error!("Invalid message = {:?}", message)
                    }
                }
                Some(f) = connections_rx.next() => {
                    framed = f;
                    info!("Link update with {} successful!!", self.id);
                }
            }
        }
    }

    fn extract_handles(
        &mut self,
    ) -> (
        Sender<(ConnectionId, RouterInMessage)>,
        Receiver<RouterOutMessage>,
    ) {
        let router_tx = self.router_tx.clone();
        let link_rx = self.link_rx.take().unwrap();
        (router_tx, link_rx)
    }
}

async fn register_with_router(
    id: u8,
    router_tx: &mut Sender<(ConnectionId, RouterInMessage)>,
) -> Receiver<RouterOutMessage> {
    let (link_tx, link_rx) = bounded(4);
    let connection = Connection {
        conn: ConnectionType::Replicator(id as usize),
        handle: link_tx,
    };
    let message = RouterInMessage::Connect(connection);
    router_tx.send((id as usize, message)).await.unwrap();
    link_rx
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
