use std::{collections::VecDeque, thread, time::Duration};

use base::{messages::Packet, pipe, EventError, EventsRx, XchgPipeA, XchgPipeB};

use crate::Event;

pub struct EventLoopSettings {
    pub max_clients: usize,
    pub max_read_buf_size: usize,
    pub max_write_buf_size: usize,
    pub max_subscriptions: usize,
    pub max_subscription_log_size: usize,
    pub max_inflight_messages: usize,
}

pub struct EventLoop {
    settings: EventLoopSettings,
    tx_pool: VecDeque<XchgPipeA<Packet>>,
    rxs: Vec<XchgPipeB<Packet>>,
    events: EventsRx<Event>,
}

impl EventLoop {
    pub fn new(settings: EventLoopSettings) -> Self {
        let mut tx_pool = VecDeque::new();
        let mut rxs = Vec::new();
        let events = EventsRx::new(settings.max_clients);

        for id in 0..settings.max_clients {
            let (a, b) = pipe(id, settings.max_read_buf_size, settings.max_write_buf_size);
            tx_pool.push_back(a);

            assert_eq!(rxs.len(), id);
            rxs.push(b);
        }

        EventLoop { settings, tx_pool, rxs, events }
    }

    pub fn start(&mut self) -> Result<(), Error> {
        loop {
            let event = self.events.poll()?;
        }
    }
}

impl Default for EventLoopSettings {
    fn default() -> Self {
        EventLoopSettings {
            max_subscriptions: 10,
            max_subscription_log_size: 100 * 1024 * 1024,
            max_inflight_messages: 100,
            max_clients: 10,
            max_read_buf_size: 1024 * 1024,
            max_write_buf_size: 1024 * 1024,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Event error")]
    EventError(#[from] EventError),
}
