use std::{collections::VecDeque, result::Result, time::Instant};

use rumq_core::*;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttConnectionStatus {
    Handshake,
    Connected,
    Disconnecting,
    Disconnected,
}

#[derive(Debug)]
pub enum Error {
    /// Broker's error reply to client's connect packet
    Connect(ConnectReturnCode),
    /// Invalid state for a given operation
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    Unsolicited,
    /// Last pingreq isn't acked
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    WrongPacket,
    /// Unsupported packet
    Unsupported,
    /// Invalid client ID
    InvalidClientId,
}

/// `MqttState` saves the state of the mqtt connection. Methods will
/// just modify the state of the object without doing any network operations
/// Methods returns so that n/w methods or n/w eventloop can
/// operate directly. This abstracts the functionality better
/// so that it's easy to switch between synchronous code, tokio (or)
/// async/await
#[derive(Debug)]
pub struct MqttState {
    /// Connection status
    connection_status: MqttConnectionStatus,
    /// Keep alive
    keep_alive: Option<Duration>,
    /// Status of last ping
    await_pingresp: bool,
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    last_pkid: PacketIdentifier,
    /// Outgoing QoS 1 publishes which aren't acked yet
    outgoing_pub: VecDeque<Publish>,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new() -> Self {
        MqttState {
            connection_status: MqttConnectionStatus::Disconnected,
            keep_alive: None,
            await_pingresp: false,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: PacketIdentifier(0),
            outgoing_pub: VecDeque::new(),
        }
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub fn handle_outgoing_mqtt_packet(&mut self, packet: Packet) -> Packet {
        let out = match packet {
            Packet::Publish(publish) => self.handle_outgoing_publish(publish),
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        out
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Packet` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub fn handle_incoming_mqtt_packet(&mut self, packet: Packet) -> Result<(Option<Packet>, Option<Packet>), Error> {
        let out = match packet {
            Packet::Publish(publish) => self.handle_incoming_publish(publish.clone()),
            Packet::Puback(pkid) => self.handle_incoming_puback(pkid),
            _ => return Err(Error::Unsupported),
        };

        self.last_incoming = Instant::now();
        out
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    pub fn handle_outgoing_publish(&mut self, publish: Publish) -> Packet {
        let publish = match publish.qos() {
            QoS::AtMostOnce => publish,
            QoS::AtLeastOnce | QoS::ExactlyOnce => self.add_packet_id_and_save(publish),
        };

        debug!(
            "Publish. Topic = {:?}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic_name(),
            publish.pkid(),
            publish.payload().len()
        );
        Packet::Publish(publish)
    }

    pub fn handle_incoming_connect(&mut self, packet: Packet) -> Result<(Option<Packet>, Option<Packet>), Error> {
        let connect = match packet {
            Packet::Connect(connect) => connect,
            packet => {
                error!("Invalid packet. Expecting connect. Received = {:?}", packet);
                self.connection_status = MqttConnectionStatus::Disconnected;
                return Err(Error::WrongPacket);
            }
        };

        if connect.client_id().starts_with(' ') || connect.client_id().is_empty() {
            error!("Client id shouldn't start with space (or) shouldn't be empty in persistent sessions");
            return Err(Error::InvalidClientId);
        }

        let connack = connack(ConnectReturnCode::Accepted, false);
        // TODO: Handle connect packet
        // TODO: Handle session present
        let reply = Some(Packet::Connack(connack));

        let notification = None;

        Ok((notification, reply))
    }

    /// Iterates through the list of stored publishes and removes the publish with the
    /// matching packet identifier. Removal is now a O(n) operation. This should be
    /// usually ok in case of acks due to ack ordering in normal conditions. But in cases
    /// where the broker doesn't guarantee the order of acks, the performance won't be optimal
    pub fn handle_incoming_puback(&mut self, pkid: PacketIdentifier) -> Result<(Option<Packet>, Option<Packet>), Error> {
        match self.outgoing_pub.iter().position(|x| *x.pkid() == Some(pkid)) {
            Some(index) => {
                let _publish = self.outgoing_pub.remove(index).expect("Wrong index");

                let request = None;
                let Packet = Some(Packet::Puback(pkid));
                Ok((Packet, request))
            }
            None => {
                error!("Unsolicited puback packet: {:?}", pkid);
                Err(Error::Unsolicited)
            }
        }
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    pub fn handle_incoming_publish(&mut self, publish: Publish) -> Result<(Option<Packet>, Option<Packet>), Error> {
        let qos = publish.qos();

        match qos {
            QoS::AtMostOnce => {
                let notification = Packet::Publish(publish);
                Ok((Some(notification), None))
            }
            QoS::AtLeastOnce => {
                let pkid = publish.pkid().unwrap();
                let request = Packet::Puback(pkid);
                let notification = Packet::Publish(publish);
                Ok((Some(notification), Some(request)))
            }
            QoS::ExactlyOnce => Err(Error::Unsupported),
        }
    }

    /// Add publish packet to the state and return the packet. This method clones the
    /// publish packet to save it to the state. This might not be a problem during low
    /// frequency/size data publishes but should ideally be `Arc`d while returning to
    /// prevent deep copy of large messages as this is anyway immutable after adding pkid
    fn add_packet_id_and_save(&mut self, mut publish: Publish) -> Publish {
        let publish = if *publish.pkid() == None {
            let pkid = self.next_pkid();
            publish.set_pkid(pkid);
            publish
        } else {
            publish
        };

        self.outgoing_pub.push_back(publish.clone());
        publish
    }

    /// Increment the packet identifier from the state and roll it when it reaches its max
    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
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

    use super::{Error, MqttConnectionStatus, MqttState, Packet};
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
        MqttState::new()
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
}
