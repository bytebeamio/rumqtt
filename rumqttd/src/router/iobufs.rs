use std::{collections::VecDeque, sync::Arc, time::Instant};

use flume::{Receiver, Sender};
use parking_lot::Mutex;

use crate::{
    protocol::Packet,
    router::{FilterIdx, MAX_CHANNEL_CAPACITY},
    Cursor, Notification,
};

use super::Forward;

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
    inflight_buffer: VecDeque<(u16, FilterIdx, Cursor)>,
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
        let inflight_buffer = VecDeque::with_capacity(MAX_INFLIGHT as usize);

        // Ensure that there won't be any new allocations
        assert!(MAX_INFLIGHT <= inflight_buffer.capacity());
        assert!(MAX_CHANNEL_CAPACITY <= data_buffer.capacity());

        let outgoing = Self {
            client_id,
            data_buffer: Arc::new(Mutex::new(data_buffer)),
            inflight_buffer,
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
            error!(
                "inflight_count = {:<2} MAX_INFLIGHT = {:<2}",
                inflight_count, MAX_INFLIGHT
            );
        }

        (buffer_count, inflight_count)
    }

    // Returns (unsolicited, outoforder) flags
    // Return: Out of order or unsolicited acks
    pub fn register_ack(&mut self, pkid: u16) -> Option<()> {
        let (head, _filter_idx, _cursor) = match self.inflight_buffer.pop_front() {
            Some(v) => v,
            None => return None,
        };

        // We don't support out of order acks
        if pkid != head {
            error!("out of order ack. pkid = {}, head = {}", pkid, head);
            return None;
        }

        Some(())
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct IncomingMeter {
    pub(crate) publish_count: usize,
    pub(crate) subscribe_count: usize,
    pub(crate) total_size: usize,
    pub(crate) last_timestamp: Instant,
    start: Instant,
    pub(crate) data_rate: usize,
}

impl Default for IncomingMeter {
    fn default() -> Self {
        Self {
            publish_count: 0,
            subscribe_count: 0,
            total_size: 0,
            last_timestamp: Instant::now(),
            start: Instant::now(),
            data_rate: 0,
        }
    }
}

impl IncomingMeter {
    //TODO: Fix this
    #[allow(dead_code)]
    pub(crate) fn average_last_data_rate(&mut self) {
        let now = Instant::now();
        self.data_rate = self.total_size / now.duration_since(self.start).as_micros() as usize;
        self.last_timestamp = now;
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct OutgoingMeter {
    pub(crate) publish_count: usize,
    pub(crate) total_size: usize,
    pub(crate) last_timestamp: Instant,
    start: Instant,
    pub(crate) data_rate: usize,
}

impl Default for OutgoingMeter {
    fn default() -> Self {
        Self {
            publish_count: 0,
            total_size: 0,
            last_timestamp: Instant::now(),
            start: Instant::now(),
            data_rate: 0,
        }
    }
}

impl OutgoingMeter {
    #[allow(dead_code)]
    pub(crate) fn average_last_data_rate(&mut self) {
        let now = Instant::now();
        self.data_rate = self.total_size / now.duration_since(self.start).as_micros() as usize;
        self.last_timestamp = now;
    }
}

#[cfg(test)]
mod test {
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
