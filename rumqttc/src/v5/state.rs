use std::{
    collections::VecDeque,
    io, mem,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use bytes::BytesMut;

use crate::v5::{outgoing_buf::OutgoingBuf, packet::*, Incoming, Request};

/// Errors during state handling
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Io Error while state is passed to network
    #[error("Io error {0:?}")]
    Io(#[from] io::Error),
    /// Broker's error reply to client's connect packet
    #[error("Connect return code `{0:?}`")]
    Connect(ConnectReturnCode),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received unsolicited ack pkid {0}")]
    Unsolicited(u16),
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
    #[error("Timeout while waiting to resolve collision")]
    CollisionTimeout,
    #[error("Mqtt serialization/deserialization error")]
    Deserialization(Error),
    #[error("Couldn't get write lock")]
    WriteLock,
}

impl From<Error> for StateError {
    fn from(e: Error) -> StateError {
        StateError::Deserialization(e)
    }
}

/// State of the mqtt connection.
// Design: Methods will just modify the state of the object without doing any network operations
// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
// This is done for 2 reasons
// Bad acks or out of order acks aren't O(n) causing cpu spikes
// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug, Clone)]
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
    /// Number of outgoing inflight publishes
    pub(crate) inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: Vec<Option<Publish>>,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: Vec<Option<u16>>,
    /// Packet ids on incoming QoS 2 publishes
    pub(crate) incoming_pub: Vec<Option<u16>>,
    /// Last collision due to broker not acking in order
    pub collision: Option<Publish>,
    /// Write buffer
    pub write: BytesMut,
    /// Indicates if acknowledgements should be send immediately
    pub manual_acks: bool,
    pub(crate) incoming_buf: Arc<Mutex<VecDeque<Incoming>>>,
    pub(crate) outgoing_buf: Arc<Mutex<OutgoingBuf>>,
    pub(crate) disconnected: Arc<RwLock<bool>>,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16, manual_acks: bool, cap: usize) -> Self {
        MqttState {
            await_pingresp: false,
            collision_ping_count: 0,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            inflight: 0,
            // index 0 is wasted as 0 is not a valid packet id
            outgoing_pub: vec![None; max_inflight as usize + 1],
            outgoing_rel: vec![None; max_inflight as usize + 1],
            incoming_pub: vec![None; std::u16::MAX as usize + 1],
            collision: None,
            // TODO: Optimize these sizes later
            write: BytesMut::with_capacity(10 * 1024),
            manual_acks,
            incoming_buf: Arc::new(Mutex::new(VecDeque::with_capacity(cap))),
            outgoing_buf: OutgoingBuf::new(max_inflight as usize),
            disconnected: Arc::new(RwLock::new(false)),
        }
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn clean(&mut self) -> Vec<Request> {
        let mut pending = Vec::with_capacity(100);
        // remove and collect pending publishes
        for publish in self.outgoing_pub.iter_mut() {
            if let Some(publish) = publish.take() {
                let request = Request::Publish(publish);
                pending.push(request);
            }
        }

        // remove and collect pending releases
        for rel in self.outgoing_rel.iter_mut() {
            if let Some(pkid) = rel.take() {
                let request = Request::PubRel(PubRel::new(pkid));
                pending.push(request);
            }
        }

        // remove packed ids of incoming qos2 publishes
        for id in self.incoming_pub.iter_mut() {
            id.take();
        }

        self.await_pingresp = false;
        self.collision_ping_count = 0;
        self.inflight = 0;
        pending
    }

    #[inline]
    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    #[inline]
    pub fn cur_pkid(&self) -> u16 {
        self.outgoing_buf.lock().unwrap().pkid_counter
    }

    #[inline]
    pub fn is_disconnected(&self) -> bool {
        *self.disconnected.read().unwrap()
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub fn handle_outgoing_packet(&mut self, request: Request) -> Result<(), StateError> {
        match request {
            Request::Publish(publish) => self.outgoing_publish(publish)?,
            Request::PubRel(pubrel) => self.outgoing_pubrel(pubrel)?,
            Request::Subscribe(subscribe) => self.outgoing_subscribe(subscribe)?,
            Request::Unsubscribe(unsubscribe) => self.outgoing_unsubscribe(unsubscribe)?,
            Request::PingReq => self.outgoing_ping()?,
            Request::Disconnect => self.outgoing_disconnect()?,
            Request::PubAck(puback) => self.outgoing_puback(puback)?,
            Request::PubRec(pubrec) => self.outgoing_pubrec(pubrec)?,
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        Ok(())
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_packet(&mut self, packet: Incoming) -> Result<(), StateError> {
        let out = match &packet {
            Incoming::PingResp => self.handle_incoming_pingresp(),
            Incoming::Publish(publish) => self.handle_incoming_publish(publish),
            Incoming::SubAck(_suback) => self.handle_incoming_suback(),
            Incoming::UnsubAck(_unsuback) => self.handle_incoming_unsuback(),
            Incoming::PubAck(puback) => self.handle_incoming_puback(puback),
            Incoming::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec),
            Incoming::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel),
            Incoming::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp),
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                return Err(StateError::WrongPacket);
            }
        };

        out?;
        self.incoming_buf.lock().unwrap().push_back(packet);
        self.last_incoming = Instant::now();
        Ok(())
    }

    #[inline]
    fn handle_incoming_suback(&mut self) -> Result<(), StateError> {
        Ok(())
    }

    #[inline]
    fn handle_incoming_unsuback(&mut self) -> Result<(), StateError> {
        Ok(())
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(&mut self, publish: &Publish) -> Result<(), StateError> {
        match publish.qos {
            QoS::AtLeastOnce if !self.manual_acks => {
                let puback = PubAck::new(publish.pkid);
                self.outgoing_puback(puback)?
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid;
                let incoming = self
                    .incoming_pub
                    .get_mut(pkid as usize)
                    .ok_or(StateError::Unsolicited(pkid))?;
                *incoming = Some(pkid);

                if !self.manual_acks {
                    let pubrec = PubRec::new(pkid);
                    self.outgoing_pubrec(pubrec)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<(), StateError> {
        let v = match mem::replace(&mut self.outgoing_pub[puback.pkid as usize], None) {
            Some(_) => {
                self.inflight -= 1;
                Ok(())
            }
            None => {
                error!("Unsolicited puback packet: {:?}", puback.pkid);
                Err(StateError::Unsolicited(puback.pkid))
            }
        };

        if let Some(publish) = self.check_collision(puback.pkid) {
            self.outgoing_pub[publish.pkid as usize] = Some(publish.clone());
            self.inflight += 1;

            publish.write(&mut self.write)?;
            self.collision_ping_count = 0;
        }

        v
    }

    fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<(), StateError> {
        match mem::replace(&mut self.outgoing_pub[pubrec.pkid as usize], None) {
            Some(_) => {
                // NOTE: Inflight - 1 for qos2 in comp
                self.outgoing_rel[pubrec.pkid as usize] = Some(pubrec.pkid);
                PubRel::new(pubrec.pkid).write(&mut self.write)?;
                Ok(())
            }
            None => {
                error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
                Err(StateError::Unsolicited(pubrec.pkid))
            }
        }
    }

    fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<(), StateError> {
        match mem::replace(&mut self.incoming_pub[pubrel.pkid as usize], None) {
            Some(_) => {
                PubComp::new(pubrel.pkid).write(&mut self.write)?;
                Ok(())
            }
            None => {
                error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
                Err(StateError::Unsolicited(pubrel.pkid))
            }
        }
    }

    fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<(), StateError> {
        if let Some(publish) = self.check_collision(pubcomp.pkid) {
            publish.write(&mut self.write)?;
            self.collision_ping_count = 0;
        }

        match mem::replace(&mut self.outgoing_rel[pubcomp.pkid as usize], None) {
            Some(_) => {
                self.inflight -= 1;
                Ok(())
            }
            None => {
                error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
                Err(StateError::Unsolicited(pubcomp.pkid))
            }
        }
    }

    #[inline]
    fn handle_incoming_pingresp(&mut self) -> Result<(), StateError> {
        self.await_pingresp = false;
        Ok(())
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn outgoing_publish(&mut self, publish: Publish) -> Result<(), StateError> {
        if publish.qos != QoS::AtMostOnce {
            // client should set proper pkid
            let pkid = publish.pkid;
            if self
                .outgoing_pub
                .get(publish.pkid as usize)
                .unwrap()
                .is_some()
            {
                info!("Collision on packet id = {:?}", publish.pkid);
                self.collision = Some(publish);
                return Ok(());
            }

            // if there is an existing publish at this pkid, this implies that broker hasn't acked this
            // packet yet. This error is possible only when broker isn't acking sequentially
            self.outgoing_pub[pkid as usize] = Some(publish.clone());
            self.inflight += 1;
        };

        debug!(
            "Publish. Topic = {}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );

        publish.write(&mut self.write)?;
        Ok(())
    }

    #[inline]
    fn outgoing_pubrel(&mut self, pubrel: PubRel) -> Result<(), StateError> {
        let pubrel = self.save_pubrel(pubrel)?;

        debug!("Pubrel. Pkid = {}", pubrel.pkid);
        PubRel::new(pubrel.pkid).write(&mut self.write)?;
        Ok(())
    }

    #[inline]
    fn outgoing_puback(&mut self, puback: PubAck) -> Result<(), StateError> {
        puback.write(&mut self.write)?;
        Ok(())
    }

    #[inline]
    fn outgoing_pubrec(&mut self, pubrec: PubRec) -> Result<(), StateError> {
        pubrec.write(&mut self.write)?;
        Ok(())
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn outgoing_ping(&mut self) -> Result<(), StateError> {
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
            "Pingreq,
            last incoming packet before {} millisecs,
            last outgoing request before {} millisecs",
            elapsed_in.as_millis(),
            elapsed_out.as_millis()
        );

        PingReq.write(&mut self.write)?;
        Ok(())
    }

    #[inline]
    fn outgoing_subscribe(&mut self, subscription: Subscribe) -> Result<(), StateError> {
        // client should set correct pkid
        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscription.filters, subscription.pkid
        );

        subscription.write(&mut self.write)?;
        Ok(())
    }

    #[inline]
    fn outgoing_unsubscribe(&mut self, unsub: Unsubscribe) -> Result<(), StateError> {
        debug!(
            "Unsubscribe. Topics = {:?}, Pkid = {:?}",
            unsub.filters, unsub.pkid
        );

        unsub.write(&mut self.write)?;
        Ok(())
    }

    #[inline]
    fn outgoing_disconnect(&mut self) -> Result<(), StateError> {
        debug!("Disconnect");

        let mut disconnected = self
            .disconnected
            .write()
            .map_err(|_| StateError::WriteLock)?;
        *disconnected = true;

        Disconnect::new().write(&mut self.write)?;
        Ok(())
    }

    #[inline]
    fn check_collision(&mut self, pkid: u16) -> Option<Publish> {
        if let Some(publish) = &self.collision {
            if publish.pkid == pkid {
                return self.collision.take();
            }
        }

        None
    }

    #[inline]
    fn save_pubrel(&mut self, pubrel: PubRel) -> Result<PubRel, StateError> {
        // pubrel's pkid should already be set correct
        self.outgoing_rel[pubrel.pkid as usize] = Some(pubrel.pkid);
        Ok(pubrel)
    }

    #[inline]
    pub fn increment_pkid(&self) -> u16 {
        self.outgoing_buf.lock().unwrap().increment_pkid()
    }

    ///// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    ///// Packet ids are incremented till maximum set inflight messages and reset to 1 after that.
    /////
    //fn next_pkid(&mut self) -> u16 {
    //    let next_pkid = self.last_pkid + 1;

    //    // When next packet id is at the edge of inflight queue,
    //    // set await flag. This instructs eventloop to stop
    //    // processing requests until all the inflight publishes
    //    // are acked
    //    if next_pkid == self.max_inflight {
    //        self.last_pkid = 0;
    //        return next_pkid;
    //    }

    //    self.last_pkid = next_pkid;
    //    next_pkid
    //}
}

#[cfg(test)]
mod test {
    use super::{MqttState, StateError};
    use crate::v5::{packet::*, Incoming, MqttOptions, Request};

    fn build_outgoing_publish(qos: QoS) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.qos = qos;
        publish
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.pkid = pkid;
        publish.qos = qos;
        publish
    }

    fn build_mqttstate() -> MqttState {
        MqttState::new(100, false, 100)
    }

    #[test]
    fn next_pkid_increments_as_expected() {
        let mqtt = build_mqttstate();

        for i in 1..=100 {
            let pkid = mqtt.increment_pkid();

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
        let mut publish = build_outgoing_publish(QoS::AtMostOnce);
        publish.pkid = 1;

        // QoS 0 publish shouldn't be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        assert_eq!(mqtt.cur_pkid(), 0);
        assert_eq!(mqtt.inflight, 0);

        // QoS1 Publish
        let mut publish = build_outgoing_publish(QoS::AtLeastOnce);
        publish.pkid = 2;

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        // cur_pkid == 0 as there is no client to update it
        assert_eq!(mqtt.cur_pkid(), 0);
        assert_eq!(mqtt.inflight, 1);

        // Packet id should be incremented and publish should be saved in queue
        publish.pkid = 3;
        mqtt.outgoing_publish(publish).unwrap();
        // cur_pkid == 0 as there is no client to update it
        assert_eq!(mqtt.cur_pkid(), 0);
        assert_eq!(mqtt.inflight, 2);

        // QoS1 Publish
        let mut publish = build_outgoing_publish(QoS::ExactlyOnce);
        publish.pkid = 4;

        // Packet id should be set and publish should be saved in queue
        mqtt.outgoing_publish(publish.clone()).unwrap();
        // cur_pkid == 0 as there is no client to update it
        assert_eq!(mqtt.cur_pkid(), 0);
        assert_eq!(mqtt.inflight, 3);

        publish.pkid = 5;
        // Packet id should be incremented and publish should be saved in queue
        mqtt.outgoing_publish(publish).unwrap();
        // cur_pkid == 0 as there is no client to update it
        assert_eq!(mqtt.cur_pkid(), 0);
        assert_eq!(mqtt.inflight, 4);
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();

        let pkid = mqtt.incoming_pub[3].unwrap();

        // only qos2 publish should be add to queue
        assert_eq!(pkid, 3);
    }

    #[test]
    fn incoming_publish_should_be_acked() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();
    }

    #[test]
    fn incoming_publish_should_not_be_acked_with_manual_acks() {
        let mut mqtt = build_mqttstate();
        mqtt.manual_acks = true;

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(&publish1).unwrap();
        mqtt.handle_incoming_publish(&publish2).unwrap();
        mqtt.handle_incoming_publish(&publish3).unwrap();

        let pkid = mqtt.incoming_pub[3].unwrap();
        assert_eq!(pkid, 3);

        assert!(mqtt.incoming_buf.lock().unwrap().is_empty());
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        mqtt.handle_incoming_publish(&publish).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let mut publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let mut publish2 = build_outgoing_publish(QoS::ExactlyOnce);
        publish1.pkid = 1;
        publish2.pkid = 2;

        mqtt.outgoing_publish(publish1).unwrap();
        mqtt.outgoing_publish(publish2).unwrap();
        assert_eq!(mqtt.inflight, 2);

        mqtt.handle_incoming_puback(&PubAck::new(1)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        mqtt.handle_incoming_puback(&PubAck::new(2)).unwrap();
        assert_eq!(mqtt.inflight, 0);

        assert!(mqtt.outgoing_pub[1].is_none());
        assert!(mqtt.outgoing_pub[2].is_none());
    }

    #[test]
    fn incoming_pubrec_should_release_publish_from_queue_and_add_relid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let mut publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let mut publish2 = build_outgoing_publish(QoS::ExactlyOnce);
        publish1.pkid = 1;
        publish2.pkid = 2;

        let _publish_out = mqtt.outgoing_publish(publish1);
        let _publish_out = mqtt.outgoing_publish(publish2);

        mqtt.handle_incoming_pubrec(&PubRec::new(2)).unwrap();
        assert_eq!(mqtt.inflight, 2);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_pub[1].clone();
        assert_eq!(backup.unwrap().pkid, 1);

        // check if the qos2 element's release pkid is 2
        assert_eq!(mqtt.outgoing_rel[2].unwrap(), 2);
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let mut publish = build_outgoing_publish(QoS::ExactlyOnce);
        publish.pkid = 1;
        mqtt.outgoing_publish(publish).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::Publish(publish) => assert_eq!(publish.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        mqtt.handle_incoming_pubrec(&PubRec::new(1)).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubRel(pubrel) => assert_eq!(pubrel.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        mqtt.handle_incoming_publish(&publish).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubRec(pubrec) => assert_eq!(pubrec.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }

        mqtt.handle_incoming_pubrel(&PubRel::new(1)).unwrap();
        let packet = read(&mut mqtt.write, 10 * 1024).unwrap();
        match packet {
            Packet::PubComp(pubcomp) => assert_eq!(pubcomp.pkid, 1),
            packet => panic!("Invalid network request: {:?}", packet),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let mut publish = build_outgoing_publish(QoS::ExactlyOnce);
        publish.pkid = 1;

        mqtt.outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(&PubRec::new(1)).unwrap();

        mqtt.handle_incoming_pubcomp(&PubComp::new(1)).unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(std::time::Duration::from_secs(10));
        mqtt.outgoing_ping().unwrap();

        // network activity other than pingresp
        let mut publish = build_outgoing_publish(QoS::AtLeastOnce);
        publish.pkid = 1;
        mqtt.handle_outgoing_packet(Request::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Incoming::PubAck(PubAck::new(1)))
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

        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(std::time::Duration::from_secs(10));

        // should ping
        mqtt.outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Incoming::PingResp).unwrap();

        // should ping
        mqtt.outgoing_ping().unwrap();
    }
}
