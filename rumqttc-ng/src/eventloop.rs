use base::{messages::Packet, EventError, EventsRx, XchgPipeB};
use tracing::debug;

use crate::Event;

pub struct EventLoopSettings {
    pub max_clients: usize,
    pub max_subscriptions: usize,
    pub max_subscription_log_size: usize,
    pub max_inflight_messages: usize,
}

pub struct EventLoop {
    settings: EventLoopSettings,
    rxs: Vec<XchgPipeB<Packet>>,
    events: EventsRx<Event>,
}

impl EventLoop {
    pub fn new(settings: EventLoopSettings) -> Self {
        let events = EventsRx::new(settings.max_clients);

        EventLoop {
            settings,
            rxs: Vec::new(),
            events,
        }
    }

    pub fn register_client(&mut self, rx: XchgPipeB<Packet>) {
        self.rxs.push(rx);
    }

    pub fn start(&mut self) -> Result<(), Error> {
        loop {
            let event = self.events.poll()?;
            debug!("Event: {:?}", event);
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
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Event error")]
    EventError(#[from] EventError),
}
