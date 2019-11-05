use crate::Request;
use crate::Notification;
use crate::MqttOptions;

use std::{
    collections::VecDeque,
    result::Result,
    time::Instant,
};

use rumq_core::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttConnectionStatus {
    Handshake,
    Connected,
    Disconnecting,
    Disconnected,
}

#[doc(hidden)]
/// Possible replys to incoming packets from the broker
#[derive(Debug)]
pub enum Reply {
    PubAck(PacketIdentifier),
    PubRec(PacketIdentifier),
    PubRel(PacketIdentifier),
    PubComp(PacketIdentifier),
}

#[derive(Debug)]
pub enum StateError {
    Unsolicited,
    AwaitPingResp
}

#[derive(Debug)]
pub struct MqttState {
    pub opts: MqttOptions,

    // --------  State  ----------
    connection_status: MqttConnectionStatus,
    await_pingresp: bool,
    last_incoming: Instant,
    last_outgoing: Instant,
    last_pkid: PacketIdentifier,

    // Stores outgoing data to handle quality of service
    outgoing_pub: VecDeque<Publish>, // QoS1 & 2 publishes
    outgoing_rel: VecDeque<PacketIdentifier>,

    // Store incoming data to handle quality of service
    incoming_pub: VecDeque<PacketIdentifier>, // QoS2 publishes
}

/// Design: `MqttState` methods will just modify the state of the object
///         but doesn't do any network operations. Methods will do
///         appropriate returns so that n/w methods or n/w eventloop can
///         operate directly. This abstracts the functionality better
///         so that it's easy to switch between synchronous code, tokio (or)
///         async/await

impl MqttState {
    pub fn new(opts: MqttOptions) -> Self {
        MqttState {
            opts,
            connection_status: MqttConnectionStatus::Disconnected,
            await_pingresp: false,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: PacketIdentifier(0),
            outgoing_pub: VecDeque::new(),
            outgoing_rel: VecDeque::new(),
            incoming_pub: VecDeque::new(),
        }
    }

    pub fn handle_outgoing_mqtt_packet(&mut self, packet: Packet) -> Request {
        let out = match packet {
            Packet::Publish(publish) => {
                let publish = self.handle_outgoing_publish(publish);
                Request::Publish(publish)
            }
            Packet::Subscribe(subs) => {
                let subscription = self.handle_outgoing_subscribe(subs);
                Request::Subscribe(subscription)
            }
            Packet::Disconnect => self.handle_outgoing_disconnect(),
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        out
    }

    // Takes incoming mqtt packet, applies state changes and returns notifiaction packet and
    // network reply packet.
    // Notification packet should be sent to the user and Mqtt reply packet which should be sent
    // back on network
    //
    // E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    // be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_mqtt_packet(&mut self, packet: Packet) -> Result<(Option<Notification>, Option<Reply>), StateError> {

        let out = match packet {
            Packet::Pingresp => self.handle_incoming_pingresp(),
            Packet::Publish(publish) => self.handle_incoming_publish(publish.clone()),
            Packet::Suback(_pkid) => Ok((None, None)),
            Packet::Unsuback(_pkid) => Ok((None, None)),
            Packet::Puback(pkid) => self.handle_incoming_puback(pkid),
            Packet::Pubrec(pkid) => self.handle_incoming_pubrec(pkid),
            Packet::Pubrel(pkid) => self.handle_incoming_pubrel(pkid),
            Packet::Pubcomp(pkid) => self.handle_incoming_pubcomp(pkid),
            _ => {
                error!("Invalid incoming paket = {:?}", packet);
                Ok((None, None))
            }
        };

        self.last_incoming = Instant::now();
        out
    }

    // pub fn handle_outgoing_connect(&mut self) -> Connect {
    //     self.connection_status = MqttConnectionStatus::Handshake;
    //     connect_packet(&self.opts)
    // }

    // pub fn handle_incoming_connack(&mut self, connack: Connack) -> Result<(), ConnectError> {
    //     let response = connack.code;
    //     if response != ConnectReturnCode::Accepted {
    //         self.connection_status = MqttConnectionStatus::Disconnected;
    //         Err(ConnectError::MqttConnectionRefused(response.to_u8()))
    //     } else {
    //         self.connection_status = MqttConnectionStatus::Connected;
    //         self.handle_previous_session();

    //         Ok(())
    //     }
    // }

    pub fn handle_outgoing_disconnect(&mut self) -> Request {
        self.connection_status = MqttConnectionStatus::Disconnecting;
        Request::Disconnect
    }

    pub fn handle_reconnection(&mut self) -> VecDeque<Request> {
        if self.opts.clean_session() {
            VecDeque::new()
        } else {
            //TODO: Write unittest for checking state during reconnection
            self.outgoing_pub.split_off(0).into_iter().map(Request::Publish).collect()
        }
    }

    fn add_packet_id_and_save(&mut self, mut publish: Publish) -> Publish {
        let publish = if publish.pkid == None {
            let pkid = self.next_pkid();
            publish.pkid = Some(pkid);
            publish
        } else {
            publish
        };

        self.outgoing_pub.push_back(publish.clone());
        publish
    }

    /// Sets next packet id if pkid is None (fresh publish) and adds it to the
    /// outgoing publish queue
    pub fn handle_outgoing_publish(&mut self, publish: Publish) -> Publish {
        
        let publish = match publish.qos {
            QoS::AtMostOnce => publish,
            QoS::AtLeastOnce | QoS::ExactlyOnce => self.add_packet_id_and_save(publish),
        };

        debug!("Publish. Topic = {:?}, Pkid = {:?}, Payload Size = {:?}", publish.topic_name, publish.pkid, publish.payload.len());
        publish
    }

    pub fn publish_queue_len(&self) -> usize {
        self.outgoing_pub.len()
    }

    pub fn is_disconnecting(&self) -> bool {
        match self.connection_status {
            MqttConnectionStatus::Disconnecting => true,
            _ => false
        }
    }

    pub fn handle_incoming_puback(&mut self, pkid: PacketIdentifier) -> Result<(Option<Notification>, Option<Reply>), StateError> {
        match self.outgoing_pub.iter().position(|x| x.pkid == Some(pkid)) {
            Some(index) => {
                let _publish = self.outgoing_pub.remove(index).expect("Wrong index");

                let request = None;
                let notification = if cfg!(feature = "acknotify") {
                    Some(Notification::PubAck(pkid))
                } else {
                   None
                };

                Ok((notification, request))
            }
            None => {
                error!("Unsolicited puback packet: {:?}", pkid);
                // let queue: VecDeque<Option<PacketIdentifier>> = self.outgoing_pub.iter().map(|p| p.pkid).collect();
                Err(StateError::Unsolicited)
            }
        }
    }

    pub fn handle_incoming_pubrec(&mut self, pkid: PacketIdentifier) -> Result<(Option<Notification>, Option<Reply>), StateError> {
        match self.outgoing_pub.iter().position(|x| x.pkid == Some(pkid)) {
            Some(index) => {
                let _publish = self.outgoing_pub.remove(index).expect("Wrong index");
                self.outgoing_rel.push_back(pkid);

                let reply = Some(Reply::PubRel(pkid));
                let notification = if cfg!(feature = "acknotify") {
                    Some(Notification::PubRec(pkid))
                } else {
                    None
                };

                Ok((notification, reply))
            }
            None => {
                error!("Unsolicited pubrec packet: {:?}", pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    // return a tuple. tuple.0 is supposed to be send to user through 'notify_tx' while tuple.1
    // should be sent back on network as ack
    pub fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(Option<Notification>, Option<Reply>), StateError> {
        let qos = publish.qos;

        match qos {
            QoS::AtMostOnce => {
                let notification = Notification::Publish(publish);
                Ok((Some(notification), None))
            }
            QoS::AtLeastOnce => {
                let pkid = publish.pkid.unwrap();
                let request = Reply::PubAck(pkid);
                let notification = Notification::Publish(publish);
                Ok((Some(notification), Some(request)))
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid.unwrap();
                let reply = Reply::PubRec(pkid);
                let notification = Notification::Publish(publish);

                self.incoming_pub.push_back(pkid);
                Ok((Some(notification), Some(reply)))
            }
        }
    }

    pub fn handle_incoming_pubrel(&mut self, pkid: PacketIdentifier) -> Result<(Option<Notification>, Option<Reply>), StateError> {
        match self.incoming_pub.iter().position(|x| *x == pkid) {
            Some(index) => {
                let _pkid = self.incoming_pub.remove(index);
                let reply = Reply::PubComp(pkid);
                Ok((None, Some(reply)))
            }
            None => {
                error!("Unsolicited pubrel packet: {:?}", pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    pub fn handle_incoming_pubcomp(&mut self, pkid: PacketIdentifier) -> Result<(Option<Notification>, Option<Reply>), StateError> {
        match self.outgoing_rel.iter().position(|x| *x == pkid) {
            Some(index) => {
                self.outgoing_rel.remove(index).expect("Wrong index");
                let notification = if cfg!(feature = "acknotify") {
                    Some(Notification::PubComp(pkid))
                } else {
                    None
                };

                Ok((notification, None))
            }
            _ => {
                error!("Unsolicited pubcomp packet: {:?}", pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    // check when the last control packet/pingreq packet
    // is received and return the status which tells if
    // keep alive time has exceeded
    // NOTE: status will be checked for zero keepalive times also
    pub fn handle_outgoing_ping(&mut self) -> Result<bool, StateError> {
        let keep_alive = self.opts.keep_alive();
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            error!("Error awaiting for last ping response");
            return Err(StateError::AwaitPingResp);
        }


        let ping = if elapsed_in > keep_alive || elapsed_out > keep_alive {
            self.await_pingresp = true;
            true
        } else {
            false
        };

        debug!(
            "Ping = {:?}. keep alive = {},
            last incoming packet before {} millisecs,
            last outgoing packet before {} millisecs",
            ping, keep_alive.as_millis(), elapsed_in.as_millis(), elapsed_out.as_millis());

        Ok(ping)
    }

    pub fn handle_incoming_pingresp(&mut self) -> Result<(Option<Notification>, Option<Reply>), StateError> {
        self.await_pingresp = false;
        Ok((None, None))
    }

    pub fn handle_outgoing_subscribe(&mut self, mut subscription: Subscribe) -> Subscribe {        
        let pkid = self.next_pkid();
        subscription.pkid = pkid;

        debug!("Subscribe. Topics = {:?}, Pkid = {:?}", subscription.topics, subscription.pkid);
        subscription
    }

    // pub fn handle_incoming_suback(&mut self, ack: Suback) -> Result<(), SubackError> {
    //     if ack.return_codes.iter().any(|v| *v == SubscribeReturnCodes::Failure) {
    //         Err(SubackError::Rejected)
    //     } else {
    //         Ok(())
    //     }
    // }

    fn handle_previous_session(&mut self) {
        self.await_pingresp = false;

        if self.opts.clean_session() {
            self.outgoing_pub.clear();
        }

        self.last_incoming = Instant::now();
        self.last_outgoing = Instant::now();
    }

    // http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    fn next_pkid(&mut self) -> PacketIdentifier {
        let PacketIdentifier(mut pkid) = self.last_pkid;
        if pkid == 65_535 {
            pkid = 0;
        }
        self.last_pkid = PacketIdentifier(pkid + 1);
        self.last_pkid
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread, time::Duration};

    use super::{MqttConnectionStatus, MqttState, StateError, Reply};
    use crate::{MqttOptions, Notification};
    use rumq_core::*;

    fn build_outgoing_publish(qos: QoS) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            pkid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        }
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            pkid: Some(PacketIdentifier(pkid)),
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        }
    }

    fn build_mqttstate() -> MqttState {
        let opts = MqttOptions::new("test-id", "127.0.0.1", 1883);
        MqttState::new(opts)
    }

    #[test]
    fn next_pkid_roll() {
        let mut mqtt = build_mqttstate();
        let mut pkt_id = PacketIdentifier(0);

        for _ in 0..65536 {
            pkt_id = mqtt.next_pkid();
        }
        assert_eq!(PacketIdentifier(1), pkt_id);
    }

    #[test]
    fn outgoing_publish_handle_should_set_pkid_correctly_and_add_publish_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // Packet id shouldn't be set and publish shouldn't be saved in queue
        let publish_out = mqtt.handle_outgoing_publish(publish);
        assert_eq!(publish_out.pkid, None);
        assert_eq!(mqtt.outgoing_pub.len(), 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        let publish_out = mqtt.handle_outgoing_publish(publish.clone());
        assert_eq!(publish_out.pkid, Some(PacketIdentifier(1)));
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        // Packet id should be incremented and publish should be saved in queue
        let publish_out = mqtt.handle_outgoing_publish(publish.clone());
        assert_eq!(publish_out.pkid, Some(PacketIdentifier(2)));
        assert_eq!(mqtt.outgoing_pub.len(), 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        let publish_out = mqtt.handle_outgoing_publish(publish.clone());
        assert_eq!(publish_out.pkid, Some(PacketIdentifier(3)));
        assert_eq!(mqtt.outgoing_pub.len(), 3);

        // Packet id should be incremented and publish should be saved in queue
        let publish_out = mqtt.handle_outgoing_publish(publish.clone());
        assert_eq!(publish_out.pkid, Some(PacketIdentifier(4)));
        assert_eq!(mqtt.outgoing_pub.len(), 4);
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

        let pkid = *mqtt.incoming_pub.get(0).unwrap();

        // only qos2 publish should be add to queue
        assert_eq!(mqtt.incoming_pub.len(), 1);
        assert_eq!(pkid, PacketIdentifier(3));
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        let (notification, request) = mqtt.handle_incoming_publish(publish).unwrap();

        match notification {
            Some(Notification::Publish(publish)) => assert_eq!(publish.pkid.unwrap(), PacketIdentifier(1)),
            _ => panic!("Invalid notification: {:?}", notification),
        }

        match request {
            Some(Reply::PubRec(PacketIdentifier(pkid))) => assert_eq!(pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.handle_outgoing_publish(publish1);
        mqtt.handle_outgoing_publish(publish2);

        mqtt.handle_incoming_puback(PacketIdentifier(1)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        let backup = mqtt.outgoing_pub.get(0).clone();
        assert_eq!(backup.unwrap().pkid, Some(PacketIdentifier(2)));

        mqtt.handle_incoming_puback(PacketIdentifier(2)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 0);
    }

    #[test]
    fn incoming_pubrec_should_release_correct_publish_from_queue_and_add_releaseid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let _publish_out = mqtt.handle_outgoing_publish(publish1);
        let _publish_out = mqtt.handle_outgoing_publish(publish2);

        mqtt.handle_incoming_pubrec(PacketIdentifier(2)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_pub.get(0).clone();
        assert_eq!(backup.unwrap().pkid, Some(PacketIdentifier(1)));

        assert_eq!(mqtt.outgoing_rel.len(), 1);

        // check if the  element's pkid is 2
        let pkid = *mqtt.outgoing_rel.get(0).unwrap();
        assert_eq!(pkid, PacketIdentifier(2));
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        mqtt.handle_outgoing_publish(publish);

        let (notification, request) = mqtt.handle_incoming_pubrec(PacketIdentifier(1)).unwrap();

        match notification {
            None => assert!(true),
            _ => panic!("Invalid notification: {:?}", notification),
        }

        match request {
            Some(Reply::PubRel(PacketIdentifier(pkid))) => assert_eq!(pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        mqtt.handle_incoming_publish(publish).unwrap();
        println!("{:?}", mqtt);
        let (notification, request) = mqtt.handle_incoming_pubrel(PacketIdentifier(1)).unwrap();

        match notification {
            None => assert!(true),
            _ => panic!("Invalid notification: {:?}", notification),
        }

        match request {
            Some(Reply::PubComp(PacketIdentifier(pkid))) => assert_eq!(pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.handle_outgoing_publish(publish);
        mqtt.handle_incoming_pubrec(PacketIdentifier(1)).unwrap();
        println!("{:?}", mqtt);

        mqtt.handle_incoming_pubcomp(PacketIdentifier(1)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        let opts = MqttOptions::new("test", "localhost", 1883).set_keep_alive(10);
        mqtt.opts = opts;
        mqtt.connection_status = MqttConnectionStatus::Connected;
        thread::sleep(Duration::from_secs(10));

        // should ping
         match  mqtt.handle_outgoing_ping().unwrap() {
            true => (),
            _ => assert!(false, "expecting ping")
        }

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.handle_outgoing_mqtt_packet(Packet::Publish(publish));
        mqtt.handle_incoming_mqtt_packet(Packet::Puback(PacketIdentifier(1))).unwrap();
        thread::sleep(Duration::from_secs(10));

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

        let opts = MqttOptions::new("test", "localhost", 1883).set_keep_alive(10);
        mqtt.opts = opts;

        mqtt.connection_status = MqttConnectionStatus::Connected;
        thread::sleep(Duration::from_secs(10));

        // should ping
        match  mqtt.handle_outgoing_ping().unwrap() {
            true => (),
            _ => assert!(false, "expecting ping")
        }
        mqtt.handle_incoming_mqtt_packet(Packet::Pingresp).unwrap();

        thread::sleep(Duration::from_secs(10));
        // should ping
         match  mqtt.handle_outgoing_ping().unwrap() {
            true => (),
            _ => assert!(false, "expecting ping")
        }
    }

    #[test]
    fn previous_session_handle_should_reset_everything_in_clean_session() {
        let mut mqtt = build_mqttstate();

        mqtt.await_pingresp = true;
        // QoS1 Publish
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pkid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish);

        mqtt.handle_previous_session();
        assert_eq!(mqtt.outgoing_pub.len(), 0);
        assert_eq!(mqtt.connection_status, MqttConnectionStatus::Disconnected);
        assert_eq!(mqtt.await_pingresp, false);
    }

    #[test]
    fn previous_session_handle_should_reset_everything_except_queues_in_persistent_session() {
        let mut mqtt = build_mqttstate();

        mqtt.await_pingresp = true;

        let opts = MqttOptions::new("test", "localhost", 1883).set_clean_session(false);
        mqtt.opts = opts;

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish);

        mqtt.handle_previous_session();
        assert_eq!(mqtt.outgoing_pub.len(), 3);
        assert_eq!(mqtt.connection_status, MqttConnectionStatus::Disconnected);
        assert_eq!(mqtt.await_pingresp, false);
    }

    // #[test]
    // fn connection_status_is_valid_while_handling_connect_and_connack_packets() {
    //     let mut mqtt = build_mqttstate();

    //     assert_eq!(mqtt.connection_status, MqttConnectionStatus::Disconnected);
    //     mqtt.handle_outgoing_connect().unwrap();
    //     assert_eq!(mqtt.connection_status, MqttConnectionStatus::Handshake);

    //     let connack = Connack {
    //         session_present: false,
    //         code: ConnectReturnCode::Accepted,
    //     };

    //     let _ = mqtt.handle_incoming_connack(connack);
    //     assert_eq!(mqtt.connection_status, MqttConnectionStatus::Connected);

    //     let connack = Connack {
    //         session_present: false,
    //         code: ConnectReturnCode::BadUsernamePassword,
    //     };

    //     let _ = mqtt.handle_incoming_connack(connack);
    //     assert_eq!(mqtt.connection_status, MqttConnectionStatus::Disconnected);
    // }

    // #[test]
    // fn connack_handle_should_not_return_list_of_incomplete_messages_to_be_sent_in_clean_session() {
    //     let mut mqtt = build_mqttstate();

    //     let publish = Publish {
    //         dup: false,
    //         qos: QoS::AtLeastOnce,
    //         retain: false,
    //         pkid: None,
    //         topic_name: "hello/world".to_owned(),
    //         payload: Arc::new(vec![1, 2, 3]),
    //     };

    //     let _ = mqtt.handle_outgoing_publish(publish.clone());
    //     let _ = mqtt.handle_outgoing_publish(publish.clone());
    //     let _ = mqtt.handle_outgoing_publish(publish);

    //     let connack = Connack {
    //         session_present: false,
    //         code: ConnectReturnCode::Accepted,
    //     };

    //     mqtt.handle_incoming_connack(connack).unwrap();
    //     let pubs = mqtt.handle_reconnection();
    //     assert_eq!(0, pubs.len());
    // }

    // #[test]
    // fn connack_handle_should_return_list_of_incomplete_messages_to_be_sent_in_persistent_session() {
    //     let mut mqtt = build_mqttstate();

    //     let opts = MqttOptions::new("test", "localhost", 1883).set_clean_session(false);
    //     mqtt.opts = opts;

    //     let publish = build_outgoing_publish(QoS::AtLeastOnce);

    //     let _ = mqtt.handle_outgoing_publish(publish.clone());
    //     let _ = mqtt.handle_outgoing_publish(publish.clone());
    //     let _ = mqtt.handle_outgoing_publish(publish);

    //     let _connack = Connack {
    //         session_present: false,
    //         code: ConnectReturnCode::Accepted,
    //     };

    //     let pubs = mqtt.handle_reconnection();
    //     assert_eq!(3, pubs.len());
    // }

    // #[test]
    // fn connect_should_respect_options() {
    //     use crate::mqttoptions::SecurityOptions::UsernamePassword;

    //     let lwt = LastWill {
    //         topic: String::from("LWT_TOPIC"),
    //         message: String::from("LWT_MESSAGE"),
    //         qos: QoS::ExactlyOnce,
    //         retain: true,
    //     };

    //     let opts = MqttOptions::new("test-id", "127.0.0.1", 1883)
    //         .set_clean_session(true)
    //         .set_keep_alive(50)
    //         .set_last_will(lwt.clone())
    //         .set_security_opts(UsernamePassword(String::from("USER"), String::from("PASS")));
    //     let mut mqtt = MqttState::new(opts);

    //     assert_eq!(mqtt.connection_status, MqttConnectionStatus::Disconnected);
    //     let pkt = mqtt.handle_outgoing_connect().unwrap();
    //     assert_eq!(
    //         pkt,
    //         Connect {
    //             protocol: Protocol::MQTT(4),
    //             keep_alive: 50,
    //             clean_session: true,
    //             client_id: String::from("test-id"),
    //             username: Some(String::from("USER")),
    //             password: Some(String::from("PASS")),
    //             last_will: Some(lwt.clone())
    //         }
    //     );
    // }
}