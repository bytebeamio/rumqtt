use rumqttc::{Incoming, Request};

use std::{time::Instant, mem};
use mqtt4bytes::*;

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Received a packet (ack) which isn't asked for
    #[error("Received a packet (ack) which isn't asked for")]
    Unsolicited,
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Collision due to broker not acking in sequence
    #[error("Broker not acking in order. Packet id collision")]
    Collision,
}

/// State of the mqtt connection.
// Design: Methods will just modify the state of the object without doing any network operations
// Design: All inflight queues are maintained in a pre initialized vec with index as packet id.
// This is done for 2 reasons
// Bad acks or out of order acks aren't O(n) causing cpu spikes
// Any missing acks from the broker are detected during the next recycled use of packet ids
#[derive(Debug, Clone)]
pub struct State {
    /// Status of last ping
    pub await_pingresp: bool,
    /// Last incoming packet time
    last_incoming: Instant,
    /// Last outgoing packet time
    last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    pub(crate) last_pkid: u16,
    /// Number of outgoing inflight publishes
    pub(crate) inflight: u16,
    /// Maximum number of allowed inflight
    pub(crate) max_inflight: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub(crate) outgoing_pub: Vec<Option<(u64, Publish)>>,
}

impl State {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new(max_inflight: u16) -> Self {
        State {
            await_pingresp: false,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            inflight: 0,
            max_inflight,
            // index 0 is wasted as 0 not a valid packet id
            outgoing_pub: vec![None; max_inflight as usize + 1],
        }
    }

    /// Returns inflight outgoing packets and clears internal queues
    pub fn _clean(&mut self) -> Vec<Request> {
        let mut pending = Vec::with_capacity(100);
        // remove and collect pending publishes
        for publish in self.outgoing_pub.iter_mut() {
            if let Some((_offset, publish)) = publish.take() {
                let request = Request::Publish(publish);
                pending.push(request);
            }
        }

        self.await_pingresp = false;
        self.inflight = 0;
        pending
    }

    pub fn inflight(&self) -> u16 {
        self.inflight
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    pub(crate) fn handle_outgoing_publish(&mut self, offset: u64, publish: Publish) -> Result<Request, StateError> {
        debug!(
            "Publish. Topic = {:?}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );

        let publish = match publish.qos {
            QoS::AtMostOnce => publish,
            QoS::AtLeastOnce | QoS::ExactlyOnce => self.add_packet_id_and_save(offset, publish)?,
        };




        Ok(Request::Publish(publish))
    }

    /// Iterates through the list of stored publishes and removes the publish with the
    /// matching packet identifier. Removal is now a O(n) operation. This should be
    /// usually ok in case of acks due to ack ordering in normal conditions. But in cases
    /// where the broker doesn't guarantee the order of acks, the performance won't be optimal
    pub(crate) fn handle_incoming_puback(&mut self, puback: PubAck) -> Result<(String, u64), StateError> {
        match mem::replace(&mut self.outgoing_pub[puback.pkid as usize], None) {
            Some((offset, publish)) => {
                self.inflight -= 1;
                Ok((publish.topic, offset))
            }
            None => {
                error!("Unsolicited puback packet: {:?}", puback.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn _handle_outgoing_ping(&mut self) -> Result<Request, StateError> {
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

    pub(crate) fn handle_incoming_pingresp(
        &mut self,
    ) -> Result<(Option<Incoming>, Option<Request>), StateError> {
        self.await_pingresp = false;
        let incoming = Some(Incoming::PingResp);
        Ok((incoming, None))
    }

    fn _handle_outgoing_subscribe(
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
    fn add_packet_id_and_save(&mut self, offset: u64, mut publish: Publish) -> Result<Publish, StateError> {
        let publish = match publish.pkid {
            // consider PacketIdentifier(0) and None as uninitialized packets
            0 => {
                let pkid = self.next_pkid();
                publish.set_pkid(pkid);
                publish
            }
            _ => publish,
        };

        // if there is an existing publish at this pkid, this implies that broker hasn't acked this
        // packet yet. Make this an error in future. This error is possible only when broker isn't
        // acking sequentially
        // TODO: Make this an error by storing replaced packet and returning an error
        let pkid = publish.pkid as usize;
        if let Some(v) = mem::replace(&mut self.outgoing_pub[pkid], Some((offset, publish.clone()))) {
            error!("Replacing unacked packet {:?}", v);
            return Err(StateError::Collision)
        }

        self.inflight += 1;
        Ok(publish)
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
        if next_pkid == self.max_inflight {
            self.last_pkid = 0;
            return next_pkid
        }

        self.last_pkid = next_pkid;
        next_pkid
    }
}
