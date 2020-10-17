use mqtt4bytes::{Packet, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, QoS};

use bytes::BytesMut;
use std::mem;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Received unsolicited ack from the device. {0}")]
    Unsolicited(u16),
    #[error("Collision with an unacked packet")]
    Serialization(mqtt4bytes::Error),
    #[error("Collision with an unacked packet")]
    Collision,
    #[error("Duplicate connect")]
    DuplicateConnect,
    #[error("Client connack")]
    ClientConnAck,
}

/// State of the mqtt connection.
/// Design: Methods will just modify the state of the object without doing any network operations
/// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
/// This is done for 2 reasons
/// Bad acks or out of order acks aren't O(n) causing cpu spikes
/// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug, Clone)]
pub struct State {
    /// Packet id of the last outgoing packet
    pub(crate) last_pkid: u16,
    /// Number of outgoing inflight publishes
    pub(crate) inflight: u16,
    /// Maximum number of allowed inflight
    pub(crate) max_inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: Vec<Option<Publish>>,
    /// Packet ids of released QoS 2 publishes
    pub(crate) outgoing_rel: Vec<Option<u16>>,
    /// Packet ids on incoming QoS 2 publishes
    pub incoming_pub: Vec<Option<u16>>,
    /// Last collision due to broker not acking in order
    pub collision: Option<Publish>,
    /// Collected incoming packets
    incoming: Vec<Packet>,
    /// Write buffer
    pub(crate) write: BytesMut,
}

impl State {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16) -> Self {
        State {
            last_pkid: 0,
            inflight: 0,
            max_inflight,
            // index 0 is wasted as 0 is not a valid packet id
            outgoing_pub: vec![None; max_inflight as usize + 1],
            outgoing_rel: vec![None; max_inflight as usize + 1],
            incoming_pub: vec![None; std::u16::MAX as usize + 1],
            collision: None,
            incoming: Vec::with_capacity(10),
            write: BytesMut::with_capacity(10 * 1024),
        }
    }

    pub fn pause_outgoing(&self) -> bool {
        self.inflight > self.max_inflight || self.collision.is_some()
    }

    pub fn take_incoming(&mut self) -> Vec<Packet> {
        mem::replace(&mut self.incoming, Vec::with_capacity(10))
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    pub(crate) fn outgoing_publish(&mut self, mut publish: Publish) -> Result<(), Error> {
        if let QoS::AtMostOnce = publish.qos {
            debug!("Publish. Qos 0. Payload size = {:?}", publish.payload.len());
            publish
                .write(&mut self.write)
                .map_err(Error::Serialization)?;

            return Ok(());
        };

        // consider PacketIdentifier(0) as uninitialized packets
        let publish = match publish.pkid {
            0 => {
                publish.pkid = self.next_pkid();
                publish
            }
            _ => publish,
        };

        let pkid = publish.pkid as usize;
        let payload_len = publish.payload.len();
        debug!("Publish. Pkid = {:?}, Size = {:?}", pkid, payload_len);

        // if there is an existing publish at this pkid, this implies that client
        // hasn't acked this packet yet. This error is possible only when client
        // isn't acking sequentially
        if self.outgoing_pub.get(publish.pkid as usize).is_some() {
            warn!("Collision on packet id = {:?}", publish.pkid);
            self.collision = Some(publish);
            return Ok(());
        }

        publish
            .write(&mut self.write)
            .map_err(Error::Serialization)?;
        self.inflight += 1;
        Ok(())
    }

    pub fn outgoing_ack(&mut self, ack: Packet) -> Result<(), Error> {
        match ack {
            Packet::PubAck(ack) => ack.write(&mut self.write),
            Packet::PubRec(ack) => ack.write(&mut self.write),
            Packet::PubComp(ack) => ack.write(&mut self.write),
            _ => unimplemented!(),
        }
        .map_err(Error::Serialization)?;
        Ok(())
    }

    pub(crate) fn handle_network_data(&mut self, packet: Packet) -> Result<(), Error> {
        match packet {
            Packet::Connect(_) => return Err(Error::DuplicateConnect),
            Packet::ConnAck(_) => return Err(Error::ClientConnAck),
            Packet::PubAck(ack) => {
                self.handle_incoming_puback(&ack)?;
            }
            Packet::PubRel(ack) => {
                self.handle_incoming_pubrel(&ack)?;
            }
            Packet::PubRec(ack) => {
                self.handle_incoming_pubrec(&ack)?;
            }
            Packet::PubComp(ack) => {
                self.handle_incoming_pubcomp(&ack)?;
            }
            Packet::PingReq => {
                self.handle_incoming_pingreq()?;
            }
            Packet::Publish(publish) => {
                self.incoming.push(Packet::Publish(publish));
            }
            Packet::Subscribe(subscribe) => {
                self.incoming.push(Packet::Subscribe(subscribe));
            }
            Packet::Disconnect => {
                // TODO Add correct disconnection handling
            }
            packet => {
                error!("Packet = {:?} not supported yet", packet);
                // return Err(Error::UnsupportedPacket(packet))
            }
        }

        Ok(())
    }

    pub(crate) fn handle_incoming_puback(&mut self, puback: &PubAck) -> Result<(), Error> {
        if let Some(publish) = self.check_collision(puback.pkid) {
            publish
                .write(&mut self.write)
                .map_err(Error::Serialization)?;
        }

        match mem::replace(&mut self.outgoing_pub[puback.pkid as usize], None) {
            Some(_) => {
                self.inflight -= 1;
                Ok(())
            }
            None => {
                error!("Unsolicited puback packet: {:?}", puback.pkid);
                Err(Error::Unsolicited(puback.pkid))
            }
        }
    }

    pub(crate) fn handle_incoming_pubrec(&mut self, pubrec: &PubRec) -> Result<(), Error> {
        match mem::replace(&mut self.outgoing_pub[pubrec.pkid as usize], None) {
            // NOTE: Inflight - 1 for qos2 in comp
            Some(_) => {
                self.outgoing_rel[pubrec.pkid as usize] = Some(pubrec.pkid);
                let pubrel = PubRel::new(pubrec.pkid);
                pubrel
                    .write(&mut self.write)
                    .map_err(Error::Serialization)?;
                Ok(())
            }
            None => {
                error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
                Err(Error::Unsolicited(pubrec.pkid))
            }
        }
    }

    pub(crate) fn handle_incoming_pubrel(&mut self, pubrel: &PubRel) -> Result<(), Error> {
        match mem::replace(&mut self.incoming_pub[pubrel.pkid as usize], None) {
            Some(_) => {
                let response = PubComp::new(pubrel.pkid);
                response
                    .write(&mut self.write)
                    .map_err(Error::Serialization)?;
                Ok(())
            }
            None => {
                error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
                Err(Error::Unsolicited(pubrel.pkid))
            }
        }
    }

    pub(crate) fn handle_incoming_pubcomp(&mut self, pubcomp: &PubComp) -> Result<(), Error> {
        if let Some(publish) = self.check_collision(pubcomp.pkid) {
            publish
                .write(&mut self.write)
                .map_err(Error::Serialization)?;
        }

        match mem::replace(&mut self.outgoing_rel[pubcomp.pkid as usize], None) {
            Some(_) => {
                self.inflight -= 1;
                Ok(())
            }
            None => {
                error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
                Err(Error::Unsolicited(pubcomp.pkid))
            }
        }
    }

    pub(crate) fn handle_incoming_pingreq(&mut self) -> Result<(), Error> {
        PingResp
            .write(&mut self.write)
            .map_err(Error::Serialization)?;

        Ok(())
    }

    fn check_collision(&mut self, pkid: u16) -> Option<Publish> {
        if let Some(publish) = &self.collision {
            // remove acked, previously collided packet from the state
            if publish.pkid == pkid {
                self.collision.take().unwrap();
                let publish = self.outgoing_pub[pkid as usize].clone().take();
                return publish;
            }
        }

        None
    }

    fn next_pkid(&mut self) -> u16 {
        let next_pkid = self.last_pkid + 1;
        if next_pkid == self.max_inflight {
            self.last_pkid = 0;
            return next_pkid;
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}
