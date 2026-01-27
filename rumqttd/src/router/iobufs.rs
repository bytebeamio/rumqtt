use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use flume::{Receiver, Sender};
use parking_lot::Mutex;
use tracing::{error, warn};

use crate::{
    protocol::Packet,
    router::{FilterIdx, MAX_CHANNEL_CAPACITY},
    Cursor, Notification,
};

use super::{Forward, IncomingMeter, OutgoingMeter};

const MAX_INFLIGHT: usize = 100;
const MAX_PKID: u16 = MAX_INFLIGHT as u16;

#[derive(Debug)]
pub struct Incoming {
    /// Identifier associated with connected client
    pub(crate) client_id: String,
    /// Recv buffer
    pub(crate) buffer: Arc<Mutex<VecDeque<Packet>>>,
    /// incoming metrics
    pub(crate) meter: IncomingMeter,
}

impl Incoming {
    #[inline]
    pub(crate) fn new(client_id: String) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_CHANNEL_CAPACITY))),
            meter: Default::default(),
            client_id,
        }
    }

    #[inline]
    pub(crate) fn buffer(&self) -> Arc<Mutex<VecDeque<Packet>>> {
        self.buffer.clone()
    }

    #[inline]
    pub(crate) fn exchange(&mut self, mut v: VecDeque<Packet>) -> VecDeque<Packet> {
        std::mem::swap(&mut v, &mut self.buffer.lock());
        v
    }
}

#[derive(Debug)]
pub struct Outgoing {
    /// Identifier associated with connected client
    pub(crate) client_id: String,
    /// Send buffer
    pub(crate) data_buffer: Arc<Mutex<VecDeque<Notification>>>,
    /// Handle which is given to router to allow router to communicate with this connection
    pub(crate) handle: Sender<()>,
    /// The buffer to keep track of inflight packets.
    inflight_buffer: VecDeque<(u16, FilterIdx, Option<Cursor>)>,
    /// PubRels waiting for PubComp
    pub(crate) unacked_pubrels: VecDeque<u16>,
    /// Last packet id
    last_pkid: u16,
    /// Metrics of outgoing messages of this connection
    pub(crate) meter: OutgoingMeter,
}

impl Outgoing {
    #[inline]
    pub(crate) fn new(client_id: String) -> (Self, Receiver<()>) {
        let (handle, rx) = flume::bounded(MAX_CHANNEL_CAPACITY);
        let data_buffer = VecDeque::with_capacity(MAX_CHANNEL_CAPACITY);
        let inflight_buffer = VecDeque::with_capacity(MAX_INFLIGHT);
        let unacked_pubrels = VecDeque::with_capacity(MAX_INFLIGHT);

        // Ensure that there won't be any new allocations
        assert!(MAX_INFLIGHT <= inflight_buffer.capacity());
        assert!(MAX_CHANNEL_CAPACITY <= data_buffer.capacity());

        let outgoing = Self {
            client_id,
            data_buffer: Arc::new(Mutex::new(data_buffer)),
            inflight_buffer,
            unacked_pubrels,
            handle,
            last_pkid: 0,
            meter: Default::default(),
        };

        (outgoing, rx)
    }

    #[inline]
    pub(crate) fn buffer(&self) -> Arc<Mutex<VecDeque<Notification>>> {
        self.data_buffer.clone()
    }

    pub fn free_slots(&self) -> usize {
        MAX_INFLIGHT - self.inflight_buffer.len()
    }

    pub fn push_notification(&mut self, notification: Notification) -> usize {
        let mut buffer = self.data_buffer.lock();
        buffer.push_back(notification);
        buffer.len()
    }

    /// Push packets to the outgoing buffer.
    pub fn push_forwards(
        &mut self,
        publishes: impl Iterator<Item = Forward>,
        qos: u8,
        filter_idx: usize,
    ) -> (usize, usize) {
        let mut buffer = self.data_buffer.lock();
        let publishes = publishes;

        if qos == 0 {
            for p in publishes {
                self.meter.publish_count += 1;
                buffer.push_back(Notification::Forward(p));
                // self.meter.total_size += p.len();
            }

            // self.meter.update_data_rate(total_size);
            let buffer_count = buffer.len();
            let inflight_count = self.inflight_buffer.len();
            return (buffer_count, inflight_count);
        }

        for mut p in publishes {
            // Index and pkid of current outgoing packet
            self.last_pkid += 1;
            p.publish.pkid = self.last_pkid;

            self.inflight_buffer
                .push_back((self.last_pkid, filter_idx, p.cursor));

            // Place max pkid packet at index 0
            if self.last_pkid == MAX_PKID {
                self.last_pkid = 0;
            }

            self.meter.publish_count += 1;
            self.meter.total_size += p.publish.topic.len() + p.publish.payload.len();
            buffer.push_back(Notification::Forward(p));
        }

        let buffer_count = buffer.len();
        let inflight_count = self.inflight_buffer.len();

        if inflight_count > MAX_INFLIGHT {
            warn!(
                "More inflight publishes than max allowed, inflight count = {}, max allowed = {}",
                inflight_count, MAX_INFLIGHT
            );
        }

        (buffer_count, inflight_count)
    }

    // Returns (unsolicited, outoforder) flags
    // Return: Out of order or unsolicited acks
    pub fn register_ack(&mut self, pkid: u16) -> Option<()> {
        let (head, _filter_idx, _cursor) = self.inflight_buffer.pop_front()?;

        // We don't support out of order acks
        if pkid != head {
            error!(pkid, head, "out of order ack.");
            return None;
        }

        Some(())
    }

    pub fn register_pubrec(&mut self, pkid: u16) {
        // NOTE: we can return true of false
        // to indicate whether this is duplicate or not
        self.unacked_pubrels.push_back(pkid);
    }

    // OASIS standards don't specify anything about ordering of PubComp
    // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901240
    // But we don't support out of order / unsolicited pubcomps
    // to be consistent with the behaviour with other acks
    pub fn register_pubcomp(&mut self, pkid: u16) -> Option<()> {
        let id = self.unacked_pubrels.pop_front()?;

        // out of order acks
        if pkid != id {
            error!(pkid, id, "out of order ack.");
            return None;
        }

        Some(())
    }

    // Here we are assuming that the first unique filter_idx we find while iterating will have the
    // least corresponding cursor because of the way we insert into the inflight_buffer
    pub fn retransmission_map(&self) -> HashMap<FilterIdx, Cursor> {
        let mut o = HashMap::new();
        for (_, filter_idx, cursor) in self.inflight_buffer.iter() {
            // if cursor in None, it means it was a retained publish
            if !o.contains_key(filter_idx) && cursor.is_some() {
                o.insert(*filter_idx, cursor.unwrap());
            }
        }

        o
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn retransmission_map_is_calculated_accurately() {
        let (mut outgoing, _) = Outgoing::new("retransmission-test".to_string());
        let mut result = HashMap::new();

        result.insert(0, (0, 8));
        result.insert(1, (0, 1));
        result.insert(2, (1, 1));
        result.insert(3, (1, 0));

        let buf = vec![
            (1, 0, Some((0, 8))),
            (1, 0, Some((0, 10))),
            (1, 1, Some((0, 1))),
            (3, 1, Some((0, 4))),
            (2, 2, Some((1, 1))),
            (1, 2, Some((2, 6))),
            (1, 2, Some((2, 1))),
            (1, 3, Some((1, 0))),
            (1, 3, Some((1, 1))),
            (1, 3, Some((1, 3))),
            (1, 3, Some((1, 3))),
        ];

        outgoing.inflight_buffer.extend(buf);
        assert_eq!(outgoing.retransmission_map(), result);
    }

    // use super::{Outgoing, MAX_INFLIGHT};
    // use crate::protocol::{Publish, QoS};
    // use crate::router::Forward;
    // use crate::Notification;
    //
    // fn publishes(count: usize) -> impl Iterator<Item = Forward> {
    //     (1..=count).map(|v| {
    //         let publish = Publish {
    //             dup: false,
    //             retain: false,
    //             pkid: 0,
    //             qos: QoS::AtLeastOnce,
    //             topic: "hello/world".into(),
    //             payload: vec![1, 2, 3].into(),
    //         };
    //
    //         Forward {
    //             cursor: (0, v as u64),
    //             publish,
    //             size: 0,
    //         }
    //     })
    // }
    //
    // #[test]
    // fn inflight_ring_buffer_pushes_correctly() {
    //     let count = MAX_INFLIGHT as usize;
    //     let (mut outgoing, _rx) = Outgoing::new("hello".to_owned());
    //     outgoing.push_forwards(publishes(count), 1, 3);
    //
    //     // Index 1 = (0, 1), Index 99 = (0, 99), Index 0 = (0, 100)
    //     assert_eq!(outgoing.inflight_buffer[0].unwrap().1, (0, count as u64));
    //     for i in 1..count {
    //         assert_eq!(outgoing.inflight_buffer[i].unwrap().1, (0, i as u64));
    //     }
    //
    //     // Outgoing publish pkids are as expected
    //     for (i, o) in outgoing.data_buffer.lock().iter().enumerate() {
    //         let pkid = match &o {
    //             Notification::Forward(f) => f.publish.pkid,
    //             _ => unreachable!(),
    //         };
    //
    //         assert_eq!(pkid, i as u16 + 1);
    //     }
    // }
    //
    // #[test]
    // fn inflight_ring_buffer_pops_correctly() {
    //     let (mut outgoing, _rx) = Outgoing::new("hello".to_owned());
    //     outgoing.push_forwards(publishes(MAX_INFLIGHT as usize), 1, 3);
    //
    //     for pkid in 1..=MAX_INFLIGHT {
    //         let (unsolicited, outoforder) = outgoing.register_ack(pkid);
    //         assert_eq!(unsolicited, false);
    //         assert_eq!(outoforder, false);
    //     }
    // }
}
