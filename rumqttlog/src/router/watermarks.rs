use crate::router::Acks;
use mqtt4bytes::*;
use std::collections::{HashMap, VecDeque};
use std::mem;

type Pkid = u16;
type Offset = u64;
type Topic = String;

/// Watermarks for a given topic
#[derive(Debug)]
pub struct Watermarks {
    pending_acks_request: Option<()>,
    /// Packet id to offset map per topic. When replication requirements
    /// are met, packet ids will be moved to acks
    pkid_offset_map: HashMap<Topic, (VecDeque<Pkid>, VecDeque<Offset>)>,
    /// Committed packet ids for acks
    acks: Vec<(Pkid, Packet)>,
    /// Offset till which replication has happened (per mesh node)
    cluster_offsets: Vec<Offset>,
}

impl Watermarks {
    pub fn new() -> Watermarks {
        Watermarks {
            pending_acks_request: None,
            pkid_offset_map: HashMap::new(),
            acks: Vec::new(),
            cluster_offsets: vec![0, 0, 0],
        }
    }

    pub fn handle_acks_request(&mut self) -> Option<Acks> {
        let acks = self.acks();
        if acks.is_empty() {
            return None;
        }

        Some(Acks::new(acks))
    }

    pub fn register_pending_acks_request(&mut self) {
        self.pending_acks_request = Some(())
    }

    pub fn take_pending_acks_request(&mut self) -> Option<()> {
        self.pending_acks_request.take()
    }

    pub fn push_publish_ack(&mut self, pkid: u16, qos: u8) {
        match qos {
            1 => self.acks.push((pkid, Packet::PubAck(PubAck::new(pkid)))),
            2 => self.acks.push((pkid, Packet::PubRec(PubRec::new(pkid)))),
            _ => return,
        }
    }

    pub fn push_subscribe_ack(&mut self, pkid: u16, return_codes: Vec<SubscribeReturnCodes>) {
        let suback = SubAck::new(pkid, return_codes);
        let suback = Packet::SubAck(suback);
        self.acks.push((pkid, suback))
    }

    pub fn push_unsubscribe_ack(&mut self, pkid: u16) {
        let unsuback = UnsubAck::new(pkid);
        let unsuback = Packet::UnsubAck(unsuback);
        self.acks.push((pkid, unsuback))
    }

    /// Returns committed acks by take
    pub fn acks(&mut self) -> Vec<(Pkid, Packet)> {
        mem::take(&mut self.acks)
    }

    pub fn update_pkid_offset_map(&mut self, topic: &str, pkid: u16, offset: u64) {
        // connection ids which are greater than supported count should be rejected during
        // connection itself. Crashing here is a bug
        let map = match self.pkid_offset_map.get_mut(topic) {
            Some(map) => map,
            None => {
                self.pkid_offset_map
                    .insert(topic.to_owned(), (VecDeque::new(), VecDeque::new()));
                self.pkid_offset_map.get_mut(topic).unwrap()
            }
        };

        map.0.push_front(pkid);
        map.1.push_front(offset);
    }
}
