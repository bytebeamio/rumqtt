use crate::{Incoming, Request};

use std::{time::Instant, mem};
use mqtt4bytes::*;

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Broker's error reply to client's connect packet
    #[error("Connect return code `{0:?}`")]
    Connect(ConnectReturnCode),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received a packet (ack) which isn't asked for")]
    Unsolicited,
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
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
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    last_pkid: u16,
    /// Number of outgoing inflight publishes
    pub(crate) inflight: usize,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: Vec<Option<Publish>>,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: Vec<Option<u16>>,
    /// Packet ids on incoming QoS 2 publishes
    pub incoming_pub: Vec<Option<u16>>,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new() -> Self {
        MqttState {
            await_pingresp: false,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            inflight: 0,
            outgoing_pub: vec![None; u16::MAX as usize + 1],
            outgoing_rel: vec![None; u16::MAX as usize + 1],
            incoming_pub: vec![None; u16::MAX as usize + 1],
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
        self.inflight = 0;
        pending
    }

    pub(crate) fn _handle_outgoing_packets(&mut self, requests: Vec<Request>) -> Result<Vec<Request>, StateError> {
        let mut out = Vec::with_capacity(10);
        for request in requests {
            let o = self.handle_outgoing_packet(request)?;
            out.push(o);
        }

        Ok(out)
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub(crate) fn handle_outgoing_packet(&mut self, request: Request,) -> Result<Request, StateError> {
        let out = match request {
            Request::Publish(publish) => self.handle_outgoing_publish(publish)?,
            Request::Subscribe(subscribe) => self.handle_outgoing_subscribe(subscribe)?,
            Request::PingReq => self.handle_outgoing_ping()?,
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        Ok(out)
    }

    pub(crate) fn handle_incoming_packets(
        &mut self,
        packets: Vec<Packet>
    ) -> Result<(Vec<Incoming>, Vec<Request>), StateError> {
        let mut incoming = Vec::with_capacity(10);
        let mut outgoing = Vec::with_capacity(10);
        for packet in packets {
            let (i, o) = self.handle_incoming_packet(packet)?;
            if let Some(i) = i {
                incoming.push(i);
            }

            if let Some(o) = o {
                outgoing.push(o);
            }
        }

        Ok((incoming, outgoing))
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub(crate) fn handle_incoming_packet(
        &mut self,
        packet: Packet,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        let out = match packet {
            Packet::PingResp => self.handle_incoming_pingresp(),
            Packet::Publish(publish) => self.handle_incoming_publish(publish),
            Packet::SubAck(suback) => self.handle_incoming_suback(suback),
            Packet::UnsubAck(unsuback) => self.handle_incoming_unsuback(unsuback),
            Packet::PubAck(puback) => self.handle_incoming_puback(puback),
            Packet::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec),
            Packet::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel),
            Packet::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp),
            _ => {
                error!("Invalid incoming packet = {:?}", packet);
                Ok((None, None))
            }
        };

        self.last_incoming = Instant::now();
        out
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn handle_outgoing_publish(&mut self, publish: Publish) -> Result<Request, StateError> {
        let publish = match publish.qos {
            QoS::AtMostOnce => publish,
            QoS::AtLeastOnce | QoS::ExactlyOnce => self.add_packet_id_and_save(publish),
        };

        debug!(
            "Publish. Topic = {:?}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );

        Ok(Request::Publish(publish))
    }

    /// Iterates through the list of stored publishes and removes the publish with the
    /// matching packet identifier. Removal is now a O(n) operation. This should be
    /// usually ok in case of acks due to ack ordering in normal conditions. But in cases
    /// where the broker doesn't guarantee the order of acks, the performance won't be optimal
    fn handle_incoming_puback(&mut self, puback: PubAck) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        match mem::replace(&mut self.outgoing_pub[puback.pkid as usize], None) {
            Some(_) => {
                self.inflight -= 1;
                let request = None;
                let incoming = Some(Incoming::PubAck(puback));
                Ok((incoming, request))
            }
            None => {
                error!("Unsolicited puback packet: {:?}", puback.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    fn handle_incoming_suback(
        &mut self,
        suback: SubAck,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        let incoming = Some(Incoming::SubAck(suback));
        let response = None;
        Ok((incoming, response))
    }

    fn handle_incoming_unsuback(
        &mut self,
        unsuback: UnsubAck,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        let incoming = Some(Incoming::UnsubAck(unsuback));
        let response = None;
        Ok((incoming, response))
    }

    fn handle_incoming_pubrec(
        &mut self,
        pubrec: PubRec,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        match mem::replace(&mut self.outgoing_pub[pubrec.pkid as usize], None) {
            Some(_) => {
                self.inflight -= 1;
                mem::replace(&mut self.outgoing_rel[pubrec.pkid as usize], Some(pubrec.pkid));
                let response = Some(Request::PubRel(PubRel::new(pubrec.pkid)));
                let incoming = Some(Incoming::PubRec(pubrec));
                Ok((incoming, response))
            }
            None => {
                error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(
        &mut self,
        publish: Publish,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        let qos = publish.qos;

        match qos {
            QoS::AtMostOnce => {
                let incoming = Incoming::Publish(publish);
                Ok((Some(incoming), None))
            }
            QoS::AtLeastOnce => {
                let pkid = publish.pkid;
                let response = Request::PubAck(PubAck::new(pkid));
                let incoming = Incoming::Publish(publish);
                Ok((Some(incoming), Some(response)))
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid;
                let response = Request::PubRec(PubRec::new(pkid));
                let incoming = Incoming::Publish(publish);
                mem::replace(&mut self.incoming_pub[pkid as usize], Some(pkid));
                Ok((Some(incoming), Some(response)))
            }
        }
    }

    fn handle_incoming_pubrel(
        &mut self,
        pubrel: PubRel,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        match mem::replace(&mut self.incoming_pub[pubrel.pkid as usize], None) {
            Some(_) => {
                let response = Request::PubComp(PubComp::new(pubrel.pkid));
                Ok((None, Some(response)))
            }
            None => {
                error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    fn handle_incoming_pubcomp(
        &mut self,
        pubcomp: PubComp,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        match mem::replace(&mut self.outgoing_rel[pubcomp.pkid as usize], None) {
            Some(_) => {
                let incoming = Some(Incoming::PubComp(pubcomp));
                let response = None;
                Ok((incoming, response))
            }
            None => {
                error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn handle_outgoing_ping(&mut self) -> Result<Request, StateError> {
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            error!("Error awaiting for last ping response");
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

        Ok(Request::PingReq)
    }

    fn handle_incoming_pingresp(
        &mut self,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        self.await_pingresp = false;
        let incoming = Some(Incoming::PingResp);
        Ok((incoming, None))
    }

    fn handle_outgoing_subscribe(
        &mut self,
        mut subscription: Subscribe,
    ) -> Result<Request, StateError> {
        let pkid = self.next_pkid();
        subscription.pkid = pkid;

        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscription.topics, subscription.pkid
        );
        Ok(Request::Subscribe(subscription))
    }

    /// Add publish packet to the state and return the packet. This method clones the
    /// publish packet to save it to the state.
    /// TODO Measure Arc vs copy perf and take a call regarding clones
    fn add_packet_id_and_save(&mut self, mut publish: Publish) -> Publish {
        let publish = match publish.pkid {
            // consider PacketIdentifier(0) and None as uninitialized packets
            0 => {
                let pkid = self.next_pkid();
                publish.set_pkid(pkid);
                publish
            }
            _ => publish,
        };

        mem::replace(&mut self.outgoing_pub[publish.pkid as usize], Some(publish.clone()));
        self.inflight += 1;
        publish
    }

    /// Increment the packet identifier from the state and roll it when it reaches its max
    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    fn next_pkid(&mut self) -> u16 {
        let mut pkid = self.last_pkid;
        if pkid == 65_535 {
            pkid = 0;
        }
        self.last_pkid = pkid + 1;
        self.last_pkid
    }
}

#[cfg(test)]
mod test {
    use super::{MqttState, Packet, StateError};
    use crate::{Incoming, MqttOptions, Request};
    use mqtt4bytes::*;

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
        MqttState::new()
    }

    #[test]
    fn next_pkid_roll() {
        let mut mqtt = build_mqttstate();
        let mut pkt_id = 0;

        for _ in 0..65536 {
            pkt_id = mqtt.next_pkid();
        }
        assert_eq!(1, pkt_id);
    }

    #[test]
    fn outgoing_publish_should_set_pkid_correctly_and_add_publish_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // Packet id shouldn't be set and publish shouldn't be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish) {
            Ok(Request::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 0);
        assert_eq!(mqtt.inflight, 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Request::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 1);
        assert_eq!(mqtt.inflight, 1);

        // Packet id should be incremented and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Request::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 2);
        assert_eq!(mqtt.inflight, 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Request::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 3);
        assert_eq!(mqtt.inflight, 3);

        // Packet id should be incremented and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Request::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 4);
        assert_eq!(mqtt.inflight, 4);
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(publish1).unwrap();
        mqtt.handle_incoming_publish(publish2).unwrap();
        mqtt.handle_incoming_publish(publish3).unwrap();

        let pkid = mqtt.incoming_pub[3].unwrap();

        // only qos2 publish should be add to queue
        assert_eq!(pkid, 3);
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        let (notification, request) = mqtt.handle_incoming_publish(publish).unwrap();

        match notification {
            Some(Incoming::Publish(publish)) => {
                assert_eq!(publish.pkid, 1)
            }
            _ => panic!("Invalid notification: {:?}", notification),
        }

        match request {
            Some(Request::PubRec(pubrec)) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.handle_outgoing_publish(publish1).unwrap();
        mqtt.handle_outgoing_publish(publish2).unwrap();
        assert_eq!(mqtt.inflight, 2);

        mqtt.handle_incoming_puback(PubAck::new(1)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        mqtt.handle_incoming_puback(PubAck::new(2)).unwrap();
        assert_eq!(mqtt.inflight, 0);

        assert!(mqtt.outgoing_pub[1].is_none());
        assert!(mqtt.outgoing_pub[2].is_none());
    }

    #[test]
    fn incoming_pubrec_should_release_correct_publish_from_queue_and_add_releaseid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let _publish_out = mqtt.handle_outgoing_publish(publish1);
        let _publish_out = mqtt.handle_outgoing_publish(publish2);

        mqtt.handle_incoming_pubrec(PubRec::new(2)).unwrap();
        assert_eq!(mqtt.inflight, 1);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_pub[1].clone();
        assert_eq!(backup.unwrap().pkid, 1);

        // check if the qos2 element's release pkid is 2
        assert_eq!(mqtt.outgoing_rel[2].unwrap(), 2);
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        mqtt.handle_outgoing_publish(publish).unwrap();

        let (notification, request) = mqtt.handle_incoming_pubrec(PubRec::new(1)).unwrap();

        match notification {
            Some(Incoming::PubRec(pubrec)) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid notification"),
        }

        match request {
            Some(Request::PubRel(pubrel)) => assert_eq!(pubrel.pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        mqtt.handle_incoming_publish(publish).unwrap();
        let (notification, request) = mqtt.handle_incoming_pubrel(PubRel::new(1)).unwrap();

        match notification {
            None => assert!(true),
            _ => panic!("Invalid notification: {:?}", notification),
        }

        match request {
            Some(Request::PubComp(pubcomp)) => assert_eq!(pubcomp.pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.handle_outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(PubRec::new(1)).unwrap();

        mqtt.handle_incoming_pubcomp(PubComp::new(1)).unwrap();
        assert_eq!(mqtt.inflight, 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(10);
        mqtt.handle_outgoing_ping().unwrap();

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.handle_outgoing_packet(Request::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Packet::PubAck(PubAck::new(1)))
            .unwrap();

        // should throw error because we didn't get pingresp for previous ping
        match mqtt.handle_outgoing_ping() {
            Ok(_) => panic!("Should throw pingresp await error"),
            Err(StateError::AwaitPingResp) => (),
            Err(e) => panic!("Should throw pingresp await error. Error = {:?}", e),
        }
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = build_mqttstate();

        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(10);

        // should ping
        mqtt.handle_outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Packet::PingResp).unwrap();

        // should ping
        mqtt.handle_outgoing_ping().unwrap();
    }
}
