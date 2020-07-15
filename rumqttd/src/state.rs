use rumqttlog::tracker::Tracker;
use rumqttc::{PubAck, Publish};

use std::mem;

pub struct State {
    tracker: Tracker,
    outgoing_pub: Vec<Option<u16>>,
    max_inflight: u16,
    last_pkid: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Received unsolicited ack from the device. {0}")]
    UnsolicitedAck(u16),
    #[error("Collision with an unacked packet")]
    Collision
}

impl State {
    pub fn new(max_inflight: u16) -> State {
        State {
            tracker: Tracker::new(),
            outgoing_pub: vec![None; max_inflight as usize],
            max_inflight,
            last_pkid: 0
        }
    }

    pub fn handle_network_puback(&mut self, ack: PubAck) -> Result<(), Error> {
        let pkid = ack.pkid as usize;
        if mem::replace(&mut self.outgoing_pub[pkid], None).is_none() {
            return Err(Error::UnsolicitedAck(ack.pkid))
        }

        Ok(())
    }

    pub fn handle_router_data(&mut self, mut publish: Publish) -> Result<Publish, Error> {
        let pkid = self.next_pkid();
        publish.set_pkid(pkid);

        // if there is an existing publish at this pkid, this implies that broker hasn't acked this
        // packet yet. Make this an error in future. This error is possible only when broker isn't
        // acking sequentially
        let index = publish.pkid as usize;
        if let Some(v) = mem::replace(&mut self.outgoing_pub[index], Some(pkid)) {
            error!("Replacing unacked packet {:?}", v);
            return Err(Error::Collision)
        }

        Ok(publish)
    }

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