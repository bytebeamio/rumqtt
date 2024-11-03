use base::{EventError, EventsRx, XchgPipeB};
use tracing::debug;

use crate::{Event, Request};

pub struct EventLoopSettings {
    pub max_clients: usize,
    pub max_subscriptions: usize,
    pub max_subscription_log_size: usize,
    pub max_inflight_messages: usize,
}

pub struct EventLoop {
    settings: EventLoopSettings,
    clients: Vec<XchgPipeB<Request>>,
    network: Option<XchgPipeB<u8>>,
    events: EventsRx<Event>,
}

impl EventLoop {
    pub fn new(settings: EventLoopSettings, events: EventsRx<Event>) -> Self {
        EventLoop {
            settings,
            clients: Vec::new(),
            network: None,
            events,
        }
    }

    pub fn register_client(&mut self, rx: XchgPipeB<Request>) {
        self.clients.push(rx);
    }

    pub fn register_network(&mut self, rx: XchgPipeB<u8>) {
        self.network = Some(rx);
    }

    pub fn start(&mut self) -> Result<(), Error> {
        let mut network = self.network.take().unwrap();
        let mut clients: Vec<_> = self.clients.drain(..).collect();

        loop {
            debug!("Polling events");
            let (id, event) = self.events.poll()?;
            debug!("Event: {:?}", &event);
            match event {
                Event::ClientData => {
                    let client = clients.get_mut(id).unwrap();
                    let mut requests = client.try_recv().unwrap();
                    requests.clear();
                    client.ack(requests);
                }
                Event::NetworkData => {
                    let packet = &mut network.recv().unwrap();
                }
            }
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
