use futures_util::sink::SinkExt;
use mqtt4bytes::*;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::sync::mpsc::error::SendError;

use std::io;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time;
use tokio_util::codec::Framed;
use crate::{IO, RouterInMessage, RouterOutMessage, DataRequest, Connection};
use crate::router::TopicsRequest;
use crate::mesh::tracker::Tracker;

#[derive(Error, Debug)]
#[error("...")]
pub enum LinkError {
    Io(#[from] io::Error),
    Mqtt4(#[from] mqtt4bytes::Error),
    Send(#[from] SendError<(String, RouterInMessage)>),
    StreamDone,
    ConnectionHandover,
    WrongPacket(Packet),
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
    id: String,
    /// Tracks the offsets and status of all the topic offsets
    tracker: Tracker,
    /// Current position in topics log
    topics_offset: TopicsRequest,
    /// Handle to send data to router
    router_tx: Sender<(String, RouterInMessage)>,
    /// Handle to this link which router uses
    link_rx: Option<Receiver<RouterOutMessage>>,
    /// Connection handle which supervisor uses to pass new connection handles
    /// Handle to supervisor
    supervisor_tx: Sender<String>,
    /// Client or server link
    is_client: bool,
}

impl Link {
    pub async fn new(mut handles: LinkConfig) -> Link {
        info!("Creating link {} with router. Client mode = {}", handles.id, handles.is_client);
        let link_rx = register_with_router(&handles.id, &mut handles.router_tx).await;
        let topics_offset = TopicsRequest {
            offset: 0,
            count: 100,
        };
        let mut wild_subscriptions = Vec::new();
        wild_subscriptions.push("#".to_owned());
        Link {
            id: handles.id,
            tracker: Tracker::new(),
            topics_offset,
            router_tx: handles.router_tx,
            link_rx: Some(link_rx),
            supervisor_tx: handles.supervisor_tx,
            is_client: handles.is_client,
        }
    }

    /// Next sweep request
    /// NOTE: When ever there is a new publish, the topics iterator gets updated and starts again
    /// from scratch. Too many new topics might affect the fairness
    async fn ask_for_more_data(&mut self, request: DataRequest) -> Result<(), LinkError> {
        // next topic's sweep request
        debug!(
            "Data request. Topic = {}, segment = {}, offset = {}, size = {}",
            request.topic, request.segment, request.offset, request.size
        );
        let request = request.clone();
        let message = RouterInMessage::DataRequest(request);
        self.router_tx.send((self.id.to_owned(), message)).await?;
        Ok(())
    }

    /// Request for topics to be polled again
    async fn ask_for_more_topics(&mut self) -> Result<(), LinkError> {
        debug!("Topics request. Offset = {}, Count = {}", self.topics_offset.offset, self.topics_offset.count);
        let message = RouterInMessage::TopicsRequest(self.topics_offset.clone());
        self.router_tx.send((self.id.to_owned(), message)).await?;
        Ok(())
    }

    /// Inform the supervisor for new connection if this is a client link. Wait for
    /// a new connection handle if this is a server link
    async fn connect<S: IO>(&mut self, connections_rx: &mut Receiver<Framed<S, MqttCodec>>) -> Framed<S, MqttCodec> {
        info!("Link with {} broken!!", self.id);
        if self.is_client {
            info!("About to make a new connection ...");
            time::delay_for(Duration::from_secs(2)).await;
            self.supervisor_tx.send(self.id.clone()).await.unwrap();
        }

        let framed = connections_rx.next().await.unwrap();
        info!("Link with {} successful!!", self.id);
        framed
    }

    /// Start handling the connection
    /// Links and connections communicate with router with a pull. This allows router to just reply.
    /// This ensured router never fills links/connections channel and take care of error handling
    pub async fn start<S: IO>(&mut self, mut connections_rx: Receiver<Framed<S, MqttCodec>>) -> Result<(), LinkError> {
        let (mut router_tx, mut link_rx) = self.extract_handles();
        let mut framed = self.connect(&mut connections_rx).await;

        self.ask_for_more_topics().await.unwrap();
        let mut topics_reply_pending = true;
        let mut all_topics_idle = false;
        let mut broken = false;
        let mut data_reply_count = 0;
        'start: loop {
            if broken {
                drop(framed);
                framed = self.connect(&mut connections_rx).await;
                broken = false;
            }

            // dbg!(topics_reply_pending, all_topics_idle, data_reply_count);
            // Ask for new topics every 200 data replys or when refresh results in 0 topics to
            // to iterate (because all the topics are in pending state) (TODO tune this number later)
            if !topics_reply_pending && (data_reply_count >= 200 || all_topics_idle) {
                self.ask_for_more_topics().await?;
                data_reply_count = 0;
                topics_reply_pending = true;
            }

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
                        Packet::Publish(publish) => {
                            let ack = Packet::PubAck(PubAck::new(publish.pkid));
                            try_loop!(framed.send(ack).await, broken, continue 'start);
                            let publish = RouterInMessage::Packet(Packet::Publish(publish));
                            router_tx.send((self.id.to_owned(), publish)).await?;
                        }
                        Packet::PubAck(_ack) => {
                            // TODO use this to inform router to release the ack to connection
                            // TODO Router will wait for enough number of replicated acks before
                            // TODO actually acking to the connection
                        }
                        packet => warn!("Received unsupported packet = {:?}", packet),
                    }
                }
                o = link_rx.recv() => {
                    let o = o.ok_or(LinkError::StreamDone)?;
                    match o {
                        // Response to previous data request. Router also returns empty responses.
                        // We use this to mark this topic as 'caught up' and move it from 'active'
                        // to 'pending'. This ensures that next topics iterator ignores this topic
                        RouterOutMessage::DataReply(reply) => {
                            // FIXME this writes corrupted packets to the n/w directly
                            for payload in reply.payload.iter() {
                                try_loop!(framed.get_mut().write_all(&payload[..]).await, broken, continue 'start);
                            }

                            data_reply_count += 1;
                            self.tracker.update(&reply.topic, reply.segment, reply.offset, reply.payload.len());
                            match self.tracker.next() {
                                Some(request) => self.ask_for_more_data(request).await?,
                                None => {
                                    all_topics_idle = true;
                                    continue
                                }
                            };
                        }
                        // Refresh the list of interested topics. Router doesn't reply empty responses.
                        // Router registers this link to notify whenever there are new topics. This
                        // behaviour is contrary to data reply where router returns empty responses
                        RouterOutMessage::TopicsReply(reply) => {
                            debug!("Topics reply. Offset = {}, Count = {}", reply.offset, reply.topics.len());
                            // TODO make a debug assert here and bring topics_reply_pending here
                            if reply.topics.len() == 0 { continue }
                            for topic in reply.topics {
                                self.tracker.match_subscription_and_add(topic);
                            }

                            // New topics. Use this to make a request to wake up request-reply loop
                            match self.tracker.next() {
                                Some(request) => {
                                    self.ask_for_more_data(request).await?;
                                }
                                None => {
                                    all_topics_idle = true;
                                    continue
                                }
                            };

                            // next topic request offset
                            self.topics_offset.offset += reply.offset + 1;
                            topics_reply_pending = false;
                        }
                    }
                }
                Some(f) = connections_rx.next() => {
                    framed = f;
                    info!("Link update with {} successful!!", self.id);
                }
            }
        }
    }

    fn extract_handles(&mut self) -> (Sender<(String, RouterInMessage)>, Receiver<RouterOutMessage>) {
        let router_tx = self.router_tx.clone();
        let link_rx = self.link_rx.take().unwrap();
        (router_tx, link_rx)
    }
}

async fn register_with_router(id: &str, router_tx: &mut Sender<(String, RouterInMessage)>) -> Receiver<RouterOutMessage> {
    let (link_tx, link_rx) = channel(4);
    let connect = Connect::new(id.clone());
    let connection = Connection {
        connect,
        handle: link_tx,
    };
    let message = RouterInMessage::Connect(connection);
    router_tx.send((id.to_string(), message)).await.unwrap();
    link_rx
}

pub struct LinkConfig {
    /// ID of the connection (which is ID of the remote router)
    pub id: String,
    /// Handle to send data to router
    pub router_tx: Sender<(String, RouterInMessage)>,
    /// Handle to supervisor
    pub supervisor_tx: Sender<String>,
    /// Marker to represent that this link is a client
    pub is_client: bool,
}

pub struct LinkHandle<S> {
    pub id: String,
    pub addr: String,
    pub connections_tx: Sender<Framed<S, MqttCodec>>,
}

impl<S: IO> LinkHandle<S> {
    pub fn new(id: String, addr: String, connections_tx: Sender<Framed<S, MqttCodec>>) -> LinkHandle<S> {
        LinkHandle {
            id,
            addr,
            connections_tx,
        }
    }
}

impl<H> Clone for LinkHandle<H> {
    fn clone(&self) -> Self {
        LinkHandle {
            id: self.id.to_string(),
            addr: self.addr.to_string(),
            connections_tx: self.connections_tx.clone()
        }
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
