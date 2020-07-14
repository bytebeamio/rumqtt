use rumqttlog::tracker::Tracker;
use rumqttc::PubAck;

use std::mem;

pub struct State {
    tracker: Tracker,
    outgoing_pub: Vec<Option<u16>>,
    max_inflight: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Received unsolicited ack from the device. {0}")]
    UnsolicitedAck(u16),
}

impl State {
    pub fn new(max_inflight: u16) -> State {
        State {
            tracker: Tracker::new(),
            outgoing_pub: vec![None; max_inflight as usize + 100],
            max_inflight
        }
    }

    pub fn handle_network_puback(&mut self, ack: PubAck) -> Result<(), Error> {
        let pkid = ack.pkid as usize;
        if mem::replace(&mut self.outgoing_pub[pkid], None).is_none() {
            return Err(Error::UnsolicitedAck(ack.pkid))
        }

        Ok(())
    }
}