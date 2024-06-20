use crate::{NoticeError, NoticeTx};

use super::mqttbytes::v5::{
    ConnAck, ConnectReturnCode, Disconnect, DisconnectReasonCode, Packet, PingReq, PubAck,
    PubAckReason, PubComp, PubCompReason, PubRec, PubRecReason, PubRel, PubRelReason, Publish,
    SubAck, Subscribe, SubscribeReasonCode, UnsubAck, UnsubAckReason, Unsubscribe,
};
use super::mqttbytes::{self, Error as MqttError, QoS};

use super::{Event, Incoming, Outgoing, Request};

use bytes::Bytes;
use fixedbitset::FixedBitSet;
use std::collections::{HashMap, VecDeque};
use std::{io, time::Instant};

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error: {0:?}")]
    Io(#[from] io::Error),
    #[error("Conversion error {0:?}")]
    Coversion(#[from] core::num::TryFromIntError),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received unsolicited ack pkid: {0}")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("A Subscribe packet must contain atleast one filter")]
    EmptySubscription,
    #[error("Mqtt serialization/deserialization error: {0}")]
    Deserialization(MqttError),
    #[error(
        "Cannot use topic alias '{alias:?}'. It's greater than the broker's maximum of '{max:?}'."
    )]
    InvalidAlias { alias: u16, max: u16 },
    #[error("Cannot send packet of size '{pkt_size:?}'. It's greater than the broker's maximum packet size of: '{max:?}'")]
    OutgoingPacketTooLarge { pkt_size: u32, max: u32 },
    #[error("Cannot receive packet of size '{pkt_size:?}'. It's greater than the client's maximum packet size of: '{max:?}'")]
    IncomingPacketTooLarge { pkt_size: usize, max: usize },
    #[error("Server sent disconnect with reason `{reason_string:?}` and code '{reason_code:?}' ")]
    ServerDisconnect {
        reason_code: DisconnectReasonCode,
        reason_string: Option<String>,
    },
    #[error("Unsubscribe failed with reason '{reason:?}' ")]
    UnsubFail { reason: UnsubAckReason },
    #[error("Subscribe failed with reason '{reason:?}' ")]
    SubFail { reason: SubscribeReasonCode },
    #[error("Publish acknowledgement failed with reason '{reason:?}' ")]
    PubAckFail { reason: PubAckReason },
    #[error("Publish receive failed with reason '{reason:?}' ")]
    PubRecFail { reason: PubRecReason },
    #[error("Publish release failed with reason '{reason:?}' ")]
    PubRelFail { reason: PubRelReason },
    #[error("Publish completion failed with reason '{reason:?}' ")]
    PubCompFail { reason: PubCompReason },
    #[error("Connection failed with reason '{reason:?}' ")]
    ConnFail { reason: ConnectReturnCode },
    #[error("Connection closed by peer abruptly")]
    ConnectionAborted,
}

impl From<mqttbytes::Error> for StateError {
    fn from(value: MqttError) -> Self {
        match value {
            MqttError::OutgoingPacketTooLarge { pkt_size, max } => {
                StateError::OutgoingPacketTooLarge { pkt_size, max }
            }
            e => StateError::Deserialization(e),
        }
    }
}

/// State of the mqtt connection.
// Design: Methods will just modify the state of the object without doing any network operations
// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
// This is done for 2 reasons
// Bad acks or out of order acks aren't O(n) causing cpu spikes
// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug)]
pub struct MqttState {
    /// Status of last ping
    pub await_pingresp: bool,
    /// Collision ping count. Collisions stop user requests
    /// which inturn trigger pings. Multiple pings without
    /// resolving collisions will result in error
    pub collision_ping_count: usize,
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    pub(crate) last_pkid: u16,
    /// Number of outgoing inflight publishes
    pub(crate) inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: HashMap<u16, (Publish, NoticeTx)>,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: HashMap<u16, NoticeTx>,
    /// Packet ids on incoming QoS 2 publishes
    pub(crate) incoming_pub: FixedBitSet,

    outgoing_sub: HashMap<u16, NoticeTx>,
    outgoing_unsub: HashMap<u16, NoticeTx>,

    /// Last collision due to broker not acking in order
    pub(crate) collision: Option<(Publish, NoticeTx)>,
    /// Buffered incoming packets
    pub events: VecDeque<Event>,
    /// Indicates if acknowledgements should be send immediately
    pub manual_acks: bool,
    /// Map of alias_id->topic
    topic_alises: HashMap<u16, Bytes>,
    /// `topic_alias_maximum` RECEIVED via connack packet
    pub broker_topic_alias_max: u16,
    /// Maximum number of allowed inflight QoS1 & QoS2 requests
    pub(crate) max_outgoing_inflight: u16,
    /// Upper limit on the maximum number of allowed inflight QoS1 & QoS2 requests
    max_outgoing_inflight_upper_limit: u16,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16, manual_acks: bool) -> Self {
        MqttState {
            await_pingresp: false,
            collision_ping_count: 0,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            inflight: 0,
            outgoing_pub: HashMap::new(),
            outgoing_rel: HashMap::new(),
            // index 0 is wasted as 0 is not a valid packet id
            incoming_pub: FixedBitSet::with_capacity(u16::MAX as usize + 1),
            outgoing_sub: HashMap::new(),
            outgoing_unsub: HashMap::new(),
            collision: None,
            // TODO: Optimize these sizes later
            events: VecDeque::with_capacity(100),
            manual_acks,
            topic_alises: HashMap::new(),
            // Set via CONNACK
            broker_topic_alias_max: 0,
            max_outgoing_inflight: max_inflight,
            max_outgoing_inflight_upper_limit: max_inflight,
        }
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn clean(&mut self) -> Vec<(NoticeTx, Request)> {
        let mut pending = Vec::with_capacity(100);
        // remove and collect pending publishes
        for (_, (publish, tx)) in self.outgoing_pub.drain() {
            let request = Request::Publish(publish);
            pending.push((tx, request));
        }

        // remove and collect pending releases
        for (pkid, tx) in self.outgoing_rel.drain() {
            let request = Request::PubRel(PubRel::new(pkid, None));
            pending.push((tx, request));
        }
        self.outgoing_rel.clear();

        // remove packed ids of incoming qos2 publishes
        self.incoming_pub.clear();

        self.await_pingresp = false;
        self.collision_ping_count = 0;
        self.inflight = 0;
        pending
    }

    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub fn handle_outgoing_packet(
        &mut self,
        tx: NoticeTx,
        request: Request,
    ) -> Result<Option<Packet>, StateError> {
        let packet = match request {
            Request::Publish(publish) => self.outgoing_publish(publish, tx)?,
            Request::PubRel(pubrel) => self.outgoing_pubrel(pubrel, tx)?,
            Request::Subscribe(subscribe) => self.outgoing_subscribe(subscribe, tx)?,
            Request::Unsubscribe(unsubscribe) => self.outgoing_unsubscribe(unsubscribe, tx)?,
            Request::PingReq => self.outgoing_ping()?,
            Request::Disconnect => {
                self.outgoing_disconnect(DisconnectReasonCode::NormalDisconnection)?
            }
            Request::PubAck(puback) => self.outgoing_puback(puback)?,
            Request::PubRec(pubrec) => self.outgoing_pubrec(pubrec)?,
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        Ok(packet)
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_packet(
        &mut self,
        mut packet: Incoming,
    ) -> Result<Option<Packet>, StateError> {
        self.events.push_back(Event::Incoming(packet.to_owned()));

        let outgoing = match &mut packet {
            Incoming::PingResp(_) => self.handle_incoming_pingresp()?,
            Incoming::Publish(publish) => self.handle_incoming_publish(publish)?,
            Incoming::SubAck(suback) => self.handle_incoming_suback(suback)?,
            Incoming::UnsubAck(unsuback) => self.handle_incoming_unsuback(unsuback)?,
            Incoming::PubAck(puback) => self.handle_incoming_puback(puback)?,
            Incoming::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec)?,
            Incoming::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel)?,
            Incoming::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp)?,
            Incoming::ConnAck(connack) => self.handle_incoming_connack(connack)?,
            Incoming::Disconnect(disconn) => self.handle_incoming_disconn(disconn)?,
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                return Err(StateError::WrongPacket);
            }
        };

        self.last_incoming = Instant::now();
        Ok(outgoing)
    }

    pub fn handle_protocol_error(&mut self) -> Result<Option<Packet>, StateError> {
        // send DISCONNECT packet with REASON_CODE 0x82
        self.outgoing_disconnect(DisconnectReasonCode::ProtocolError)
    }

    fn handle_incoming_suback(
        &mut self,
        suback: &mut SubAck,
    ) -> Result<Option<Packet>, StateError> {
        if suback.pkid > self.max_outgoing_inflight {
            error!("Unsolicited suback packet: {:?}", suback.pkid);
            return Err(StateError::Unsolicited(suback.pkid));
        }

        let tx = self
            .outgoing_sub
            .remove(&suback.pkid)
            .ok_or(StateError::Unsolicited(suback.pkid))?;

        for reason in suback.return_codes.iter() {
            match reason {
                SubscribeReasonCode::Success(qos) => {
                    debug!("SubAck Pkid = {:?}, QoS = {:?}", suback.pkid, qos);
                }
                _ => {
                    tx.error(NoticeError::V5Subscribe(*reason));
                    return Err(StateError::SubFail { reason: *reason });
                }
            }
        }
        tx.success();

        Ok(None)
    }

    fn handle_incoming_unsuback(
        &mut self,
        unsuback: &mut UnsubAck,
    ) -> Result<Option<Packet>, StateError> {
        if unsuback.pkid > self.max_outgoing_inflight {
            error!("Unsolicited unsuback packet: {:?}", unsuback.pkid);
            return Err(StateError::Unsolicited(unsuback.pkid));
        }

        let tx = self
            .outgoing_unsub
            .remove(&unsuback.pkid)
            .ok_or(StateError::Unsolicited(unsuback.pkid))?;

        for reason in unsuback.reasons.iter() {
            if reason != &UnsubAckReason::Success {
                tx.error(NoticeError::V5Unsubscribe(*reason));
                return Err(StateError::UnsubFail { reason: *reason });
            }
        }
        tx.success();

        Ok(None)
    }

    fn handle_incoming_connack(
        &mut self,
        connack: &mut ConnAck,
    ) -> Result<Option<Packet>, StateError> {
        if connack.code != ConnectReturnCode::Success {
            return Err(StateError::ConnFail {
                reason: connack.code,
            });
        }

        if let Some(props) = &connack.properties {
            if let Some(topic_alias_max) = props.topic_alias_max {
                self.broker_topic_alias_max = topic_alias_max
            }

            if let Some(max_inflight) = props.receive_max {
                self.max_outgoing_inflight =
                    max_inflight.min(self.max_outgoing_inflight_upper_limit);
                // FIXME: Maybe resize the pubrec and pubrel queues here
                // to save some space.
            }
        }
        Ok(None)
    }

    fn handle_incoming_disconn(
        &mut self,
        disconn: &mut Disconnect,
    ) -> Result<Option<Packet>, StateError> {
        let reason_code = disconn.reason_code;
        let reason_string = if let Some(props) = &disconn.properties {
            props.reason_string.clone()
        } else {
            None
        };
        Err(StateError::ServerDisconnect {
            reason_code,
            reason_string,
        })
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(
        &mut self,
        publish: &mut Publish,
    ) -> Result<Option<Packet>, StateError> {
        let qos = publish.qos;

        let topic_alias = match &publish.properties {
            Some(props) => props.topic_alias,
            None => None,
        };

        if !publish.topic.is_empty() {
            if let Some(alias) = topic_alias {
                self.topic_alises.insert(alias, publish.topic.clone());
            }
        } else if let Some(alias) = topic_alias {
            if let Some(topic) = self.topic_alises.get(&alias) {
                topic.clone_into(&mut publish.topic);
            } else {
                self.handle_protocol_error()?;
            };
        }

        match qos {
            QoS::AtMostOnce => Ok(None),
            QoS::AtLeastOnce => {
                if !self.manual_acks {
                    let puback = PubAck::new(publish.pkid, None);
                    return self.outgoing_puback(puback);
                }
                Ok(None)
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid;
                self.incoming_pub.insert(pkid as usize);

                if !self.manual_acks {
                    let pubrec = PubRec::new(pkid, None);
                    return self.outgoing_pubrec(pubrec);
                }
                Ok(None)
            }
        }
    }

    fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<Option<Packet>, StateError> {
        if puback.pkid > self.max_outgoing_inflight {
            error!("Unsolicited puback packet: {:?}", puback.pkid);
            return Err(StateError::Unsolicited(puback.pkid));
        }

        let (_, tx) = self
            .outgoing_pub
            .remove(&puback.pkid)
            .ok_or(StateError::Unsolicited(puback.pkid))?;
        tx.success();

        self.inflight -= 1;

        if puback.reason != PubAckReason::Success
            && puback.reason != PubAckReason::NoMatchingSubscribers
        {
            return Err(StateError::PubAckFail {
                reason: puback.reason,
            });
        }

        let packet = self.check_collision(puback.pkid).map(|(publish, tx)| {
            self.outgoing_pub
                .insert(publish.pkid, (publish.clone(), tx));
            self.inflight += 1;

            let event = Event::Outgoing(Outgoing::Publish(publish.pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;

            Packet::Publish(publish)
        });

        Ok(packet)
    }

    fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<Option<Packet>, StateError> {
        if pubrec.pkid > self.max_outgoing_inflight {
            error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
            return Err(StateError::Unsolicited(pubrec.pkid));
        }

        let (_, tx) = self
            .outgoing_pub
            .remove(&pubrec.pkid)
            .ok_or(StateError::Unsolicited(pubrec.pkid))?;

        if pubrec.reason != PubRecReason::Success
            && pubrec.reason != PubRecReason::NoMatchingSubscribers
        {
            return Err(StateError::PubRecFail {
                reason: pubrec.reason,
            });
        }

        // NOTE: Inflight - 1 for qos2 in comp
        self.outgoing_rel.insert(pubrec.pkid, tx);
        let event = Event::Outgoing(Outgoing::PubRel(pubrec.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRel(PubRel::new(pubrec.pkid, None))))
    }

    fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<Option<Packet>, StateError> {
        if !self.incoming_pub.contains(pubrel.pkid as usize) {
            error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
            return Err(StateError::Unsolicited(pubrel.pkid));
        }
        self.incoming_pub.set(pubrel.pkid as usize, false);

        if pubrel.reason != PubRelReason::Success {
            return Err(StateError::PubRelFail {
                reason: pubrel.reason,
            });
        }

        let event = Event::Outgoing(Outgoing::PubComp(pubrel.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubComp(PubComp::new(pubrel.pkid, None))))
    }

    fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<Option<Packet>, StateError> {
        if pubcomp.pkid > self.max_outgoing_inflight {
            error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
            return Err(StateError::Unsolicited(pubcomp.pkid));
        }
        self.outgoing_rel
            .remove(&pubcomp.pkid)
            .ok_or(StateError::Unsolicited(pubcomp.pkid))?
            .success();

        self.inflight -= 1;
        let packet = self.check_collision(pubcomp.pkid).map(|(publish, tx)| {
            self.outgoing_pub
                .insert(pubcomp.pkid, (publish.clone(), tx));
            let event = Event::Outgoing(Outgoing::Publish(publish.pkid));
            self.events.push_back(event);
            self.collision_ping_count = 0;

            Packet::Publish(publish)
        });

        Ok(packet)
    }

    fn handle_incoming_pingresp(&mut self) -> Result<Option<Packet>, StateError> {
        self.await_pingresp = false;
        Ok(None)
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn outgoing_publish(
        &mut self,
        mut publish: Publish,
        notice_tx: NoticeTx,
    ) -> Result<Option<Packet>, StateError> {
        if publish.qos != QoS::AtMostOnce {
            if publish.pkid == 0 {
                publish.pkid = self.next_pkid();
            }

            let pkid = publish.pkid;

            if self.outgoing_pub.get(&publish.pkid).is_some() {
                info!("Collision on packet id = {:?}", publish.pkid);
                self.collision = Some((publish, notice_tx));
                let event = Event::Outgoing(Outgoing::AwaitAck(pkid));
                self.events.push_back(event);
                return Ok(None);
            }

            // if there is an existing publish at this pkid, this implies that broker hasn't acked this
            // packet yet. This error is possible only when broker isn't acking sequentially
            self.outgoing_pub.insert(pkid, (publish.clone(), notice_tx));
            self.inflight += 1;
        } else {
            notice_tx.success()
        }

        debug!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            String::from_utf8(publish.topic.to_vec()).unwrap(),
            publish.pkid,
            publish.payload.len()
        );

        let pkid = publish.pkid;
        if let Some(props) = &publish.properties {
            if let Some(alias) = props.topic_alias {
                if alias > self.broker_topic_alias_max {
                    // We MUST NOT send a Topic Alias that is greater than the
                    // broker's Topic Alias Maximum.
                    return Err(StateError::InvalidAlias {
                        alias,
                        max: self.broker_topic_alias_max,
                    });
                }
            }
        };

        let event = Event::Outgoing(Outgoing::Publish(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Publish(publish)))
    }

    fn outgoing_pubrel(
        &mut self,
        pubrel: PubRel,
        notice_tx: NoticeTx,
    ) -> Result<Option<Packet>, StateError> {
        let pubrel = self.save_pubrel(pubrel, notice_tx)?;

        debug!("Pubrel. Pkid = {}", pubrel.pkid);

        let event = Event::Outgoing(Outgoing::PubRel(pubrel.pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRel(PubRel::new(pubrel.pkid, None))))
    }

    fn outgoing_puback(&mut self, puback: PubAck) -> Result<Option<Packet>, StateError> {
        let pkid = puback.pkid;
        let event = Event::Outgoing(Outgoing::PubAck(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubAck(puback)))
    }

    fn outgoing_pubrec(&mut self, pubrec: PubRec) -> Result<Option<Packet>, StateError> {
        let pkid = pubrec.pkid;
        let event = Event::Outgoing(Outgoing::PubRec(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::PubRec(pubrec)))
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn outgoing_ping(&mut self) -> Result<Option<Packet>, StateError> {
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        if self.collision.is_some() {
            self.collision_ping_count += 1;
            if self.collision_ping_count >= 2 {
                return Err(StateError::CollisionTimeout);
            }
        }

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            return Err(StateError::AwaitPingResp);
        }

        self.await_pingresp = true;

        debug!(
            "Pingreq, last incoming packet before {:?}, last outgoing request before {:?}",
            elapsed_in, elapsed_out,
        );

        let event = Event::Outgoing(Outgoing::PingReq);
        self.events.push_back(event);

        Ok(Some(Packet::PingReq(PingReq)))
    }

    fn outgoing_subscribe(
        &mut self,
        mut subscription: Subscribe,
        notice_tx: NoticeTx,
    ) -> Result<Option<Packet>, StateError> {
        if subscription.filters.is_empty() {
            return Err(StateError::EmptySubscription);
        }

        let pkid = self.next_pkid();
        subscription.pkid = pkid;

        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscription.filters, subscription.pkid
        );

        self.outgoing_sub.insert(pkid, notice_tx);
        let event = Event::Outgoing(Outgoing::Subscribe(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Subscribe(subscription)))
    }

    fn outgoing_unsubscribe(
        &mut self,
        mut unsub: Unsubscribe,
        notice_tx: NoticeTx,
    ) -> Result<Option<Packet>, StateError> {
        let pkid = self.next_pkid();
        unsub.pkid = pkid;

        debug!(
            "Unsubscribe. Topics = {:?}, Pkid = {:?}",
            unsub.filters, unsub.pkid
        );

        self.outgoing_unsub.insert(pkid, notice_tx);
        let event = Event::Outgoing(Outgoing::Unsubscribe(pkid));
        self.events.push_back(event);

        Ok(Some(Packet::Unsubscribe(unsub)))
    }

    fn outgoing_disconnect(
        &mut self,
        reason: DisconnectReasonCode,
    ) -> Result<Option<Packet>, StateError> {
        debug!("Disconnect with {:?}", reason);
        let event = Event::Outgoing(Outgoing::Disconnect);
        self.events.push_back(event);

        Ok(Some(Packet::Disconnect(Disconnect::new(reason))))
    }

    fn check_collision(&mut self, pkid: u16) -> Option<(Publish, NoticeTx)> {
        if let Some((publish, _)) = &self.collision {
            if publish.pkid == pkid {
                return self.collision.take();
            }
        }

        None
    }

    fn save_pubrel(
        &mut self,
        mut pubrel: PubRel,
        notice_tx: NoticeTx,
    ) -> Result<PubRel, StateError> {
        let pubrel = match pubrel.pkid {
            // consider PacketIdentifier(0) as uninitialized packets
            0 => {
                pubrel.pkid = self.next_pkid();
                pubrel
            }
            _ => pubrel,
        };

        self.outgoing_rel.insert(pubrel.pkid, notice_tx);
        self.inflight += 1;
        Ok(pubrel)
    }

    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    /// Packet ids are incremented till maximum set inflight messages and reset to 1 after that.
    ///
    fn next_pkid(&mut self) -> u16 {
        let next_pkid = self.last_pkid + 1;

        // When next packet id is at the edge of inflight queue,
        // set await flag. This instructs eventloop to stop
        // processing requests until all the inflight publishes
        // are acked
        if next_pkid == self.max_outgoing_inflight {
            self.last_pkid = 0;
            return next_pkid;
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}

#[cfg(test)]
mod test {
    use crate::notice::NoticeTx;

    use super::mqttbytes::v5::*;
    use super::mqttbytes::*;
    use super::{Event, Incoming, Outgoing, Request};
    use super::{MqttState, StateError};

    fn build_outgoing_publish(qos: QoS) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, None);
        publish.qos = qos;
        publish
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload, None);
        publish.pkid = pkid;
        publish.qos = qos;
        publish
    }

    fn build_mqttstate() -> MqttState {
        MqttState::new(u16::MAX, false)
    }

    #[test]
    fn next_pkid_increments_as_expected() {
        let mut mqtt = build_mqttstate();

        for i in 1..=100 {
            let pkid = mqtt.next_pkid();

            // loops between 0-99. % 100 == 0 implies border
            let expected = i % 100;
            if expected == 0 {
                break;
            }

            assert_eq!(expected, pkid);
        }
    }

    #[test]
    fn outgoing_publish_should_set_pkid_and_add_publish_to_queue() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // QoS 0 publish shouldn't be saved in queue
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish, tx).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight, 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish.clone(), tx).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 1);

        // Packet id should be incremented and publish should be saved in queue
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish, tx).unwrap();
        assert_eq!(mqtt.last_pkid, 2);
        assert_eq!(mqtt.inflight, 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish.clone(), tx).unwrap();
        assert_eq!(mqtt.last_pkid, 3);
        assert_eq!(mqtt.inflight, 3);

        // Packet id should be incremented and publish should be saved in queue
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish, tx).unwrap();
        assert_eq!(mqtt.last_pkid, 4);
        assert_eq!(mqtt.inflight, 4);
    }

    #[test]
    fn outgoing_publish_with_max_inflight_is_ok() {
        let mut mqtt = MqttState::new(2, false);

        // QoS2 publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish.clone(), tx).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 1);

        // Packet id should be set back down to 0, since we hit the limit
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish.clone(), tx).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight, 2);

        // This should cause a collition
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish.clone(), tx).unwrap();
        assert_eq!(mqtt.last_pkid, 1);
        assert_eq!(mqtt.inflight, 2);
        assert!(mqtt.collision.is_some());

        mqtt.handle_incoming_puback(&PubAck::new(1, None)).unwrap();
        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        // Now there should be space in the outgoing queue
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish.clone(), tx).unwrap();
        assert_eq!(mqtt.last_pkid, 0);
        assert_eq!(mqtt.inflight, 2);
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        // only qos2 publish should be add to queue
        assert!(mqtt.incoming_pub.contains(3));
    }

    #[test]
    fn incoming_publish_should_be_acked() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        if let Event::Outgoing(Outgoing::PubAck(pkid)) = mqtt.events[0] {
            assert_eq!(pkid, 2);
        } else {
            panic!("missing puback");
        }

        if let Event::Outgoing(Outgoing::PubRec(pkid)) = mqtt.events[1] {
            assert_eq!(pkid, 3);
        } else {
            panic!("missing PubRec");
        }
    }

    #[test]
    fn incoming_publish_should_not_be_acked_with_manual_acks() {
        let mut mqtt = build_mqttstate();
        mqtt.manual_acks = true;

        // QoS0, 1, 2 Publishes
        let mut publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let mut publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let mut publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&mut publish1).unwrap();
        mqtt.handle_incoming_publish(&mut publish2).unwrap();
        mqtt.handle_incoming_publish(&mut publish3).unwrap();

        assert!(mqtt.incoming_pub.contains(3));
        assert!(mqtt.events.is_empty());
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let mut publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        match mqtt.handle_incoming_publish(&mut publish).unwrap().unwrap() {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish1, tx).unwrap();
        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish2, tx).unwrap();
        assert_eq!(mqtt.inflight, 2);

        mqtt.handle_incoming_puback(&PubAck::new(1, None)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        mqtt.handle_incoming_puback(&PubAck::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 0);

        assert!(mqtt.outgoing_pub.get(&1).is_none());
        assert!(mqtt.outgoing_pub.get(&2).is_none());
    }

    #[test]
    fn incoming_puback_with_pkid_greater_than_max_inflight_should_be_handled_gracefully() {
        let mut mqtt = build_mqttstate();

        let got = mqtt
            .handle_incoming_puback(&PubAck::new(101, None))
            .unwrap_err();

        match got {
            StateError::Unsolicited(pkid) => assert_eq!(pkid, 101),
            e => panic!("Unexpected error: {}", e),
        }
    }

    #[test]
    fn incoming_pubrec_should_release_publish_from_queue_and_add_relid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let (tx, _) = NoticeTx::new();
        let _publish_out = mqtt.outgoing_publish(publish1, tx);
        let (tx, _) = NoticeTx::new();
        let _publish_out = mqtt.outgoing_publish(publish2, tx);

        mqtt.handle_incoming_pubrec(&PubRec::new(2, None)).unwrap();
        assert_eq!(mqtt.inflight, 2);

        // check if the remaining element's pkid is 1
        let (backup, _) = mqtt.outgoing_pub.get(&1).unwrap();
        assert_eq!(backup.pkid, 1);

        // check if the qos2 element's release pkik has been set
        assert!(mqtt.outgoing_rel.get(&2).is_some());
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        let (tx, _) = NoticeTx::new();
        match mqtt.outgoing_publish(publish, tx).unwrap().unwrap() {
            Packet::Publish(publish) => assert_eq!(publish.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        match mqtt
            .handle_incoming_pubrec(&PubRec::new(1, None))
            .unwrap()
            .unwrap()
        {
            Packet::PubRel(pubrel) => assert_eq!(pubrel.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let mut publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        match mqtt.handle_incoming_publish(&mut publish).unwrap().unwrap() {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        match mqtt
            .handle_incoming_pubrel(&PubRel::new(1, None))
            .unwrap()
            .unwrap()
        {
            Packet::PubComp(pubcomp) => assert_eq!(pubcomp.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        let (tx, _) = NoticeTx::new();
        mqtt.outgoing_publish(publish, tx).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec::new(1, None)).unwrap();

        mqtt.handle_incoming_pubcomp(&PubComp::new(1, None))
            .unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        mqtt.outgoing_ping().unwrap();

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        let (tx, _) = NoticeTx::new();
        mqtt.handle_outgoing_packet(tx, Request::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Incoming::PubAck(PubAck::new(1, None)))
            .unwrap();

        // should throw error because we didn't get pingresp for previous ping
        match mqtt.outgoing_ping() {
            Ok(_) => panic!("Should throw pingresp await error"),
            Err(StateError::AwaitPingResp) => (),
            Err(e) => panic!("Should throw pingresp await error. Error = {:?}", e),
        }
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = build_mqttstate();

        // should ping
        mqtt.outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Incoming::PingResp(PingResp))
            .unwrap();

        // should ping
        mqtt.outgoing_ping().unwrap();
    }
}
