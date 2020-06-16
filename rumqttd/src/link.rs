use rumqttlog::mqtt4bytes::*;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::sync::mpsc::error::SendError;

use std::io;

use futures_util::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::Framed;
use crate::IO;
use crate::tracker::Tracker;
use rumqttlog::router::TopicsRequest;
use rumqttlog::{RouterInMessage, RouterOutMessage, DataRequest};

#[derive(Error, Debug)]
#[error("...")]
pub enum LinkError {
    Io(#[from] io::Error),
    Mqtt4(#[from] rumqttlog::mqtt4bytes::Error),
    Send(#[from] SendError<(String, RouterInMessage)>),
    ChannelClosed,
    NetworkClosed,
    WrongPacket(Packet),
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
}

impl Link {
    /// Creates a new link with a remote client. Takes in handle to send data to router (router_tx)
    /// and another handle to receive data from router
    pub fn new(
        id: &str,
        router_tx: Sender<(String, RouterInMessage)>,
        link_rx: Receiver<RouterOutMessage>
    ) -> Link {
        info!("Creating link {} with router", id);
        let topics_offset = TopicsRequest { offset: 0, count: 100 };
        let mut wild_subscriptions = Vec::new();
        wild_subscriptions.push("#".to_owned());
        Link {
            id: id.to_owned(),
            tracker: Tracker::new(),
            topics_offset,
            router_tx,
            link_rx: Some(link_rx),
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

    /// Start handling the connection
    /// Links and connections communicate with router with a pull. This allows router to just reply.
    /// This ensured router never fills links/connections channel and take care of error handling
    pub async fn start<S: IO>(&mut self, mut framed: Framed<S, MqttCodec>) -> Result<(), LinkError> {
        let (mut router_tx, mut link_rx) = self.extract_handles();
        self.ask_for_more_topics().await.unwrap();

        let mut topics_reply_pending = true;
        let mut all_topics_idle = false;
        let mut data_reply_count = 0;

        loop {
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
                    let packet = o.ok_or(LinkError::NetworkClosed)??;
                    match packet {
                        Packet::Publish(publish) => {
                            let publish = RouterInMessage::Packet(Packet::Publish(publish));
                            router_tx.send((self.id.to_owned(), publish)).await?;
                        }
                        Packet::PubAck(_) => {
                            // Use this for flow control to stop asking router for more data when
                            // acks aren't being received
                        }
                        packet => warn!("Received unsupported packet = {:?}", packet),
                    }
                }
                o = link_rx.next() => {
                    let message = o.ok_or(LinkError::ChannelClosed)?;
                    match message {
                        RouterOutMessage::DataReply(reply) => {
                            // FIXME this writes corrupted packets to the n/w directly
                            for payload in reply.payload.iter() {
                                framed.get_mut().write_all(&payload[..]).await?;
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
                                Some(request) => self.ask_for_more_data(request).await?,
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
            }
        }
    }

    fn extract_handles(&mut self) -> (Sender<(String, RouterInMessage)>, Receiver<RouterOutMessage>) {
        let router_tx = self.router_tx.clone();
        let link_rx = self.link_rx.take().unwrap();
        (router_tx, link_rx)
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
