use std::thread;

use base::{pipe, EventsRx};
use client::blocking::Client;
use eventloop::EventLoop;
pub use eventloop::EventLoopSettings;
pub use transport::TransportSettings;

use crate::{client, eventloop, transport, Tx};

pub struct Builder {
    eventloop_settings: EventLoopSettings,
    transport_settings: TransportSettings,
    clients: Vec<(usize, usize)>, // client id, buffer size
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            eventloop_settings: EventLoopSettings::default(),
            transport_settings: TransportSettings::default(),
            clients: Vec::new(),
        }
    }

    // TODO: Remove id
    pub fn register_client(mut self, id: usize, size: usize) -> Self {
        self.clients.push((id, size));
        self
    }

    pub fn set_eventloop(mut self, settings: EventLoopSettings) -> Self {
        self.eventloop_settings = settings;
        self
    }

    pub fn set_transport(mut self, settings: TransportSettings) -> Self {
        self.transport_settings = settings;
        self
    }

    pub fn start(self) -> Vec<Client> {
        let events = EventsRx::new(self.eventloop_settings.max_subscriptions);
        let mut clients = Vec::with_capacity(self.clients.len());
        let mut eventloop = EventLoop::new(self.eventloop_settings);
        let mut rxs = Vec::with_capacity(self.clients.len());

        // Create clients
        for (id, size) in self.clients {
            let (a, b) = pipe(id, size, size);

            let tx = Tx {
                events_tx: events.producer(id),
                tx: a,
            };

            rxs.push(b);
            clients.push(Client::new(id, tx));
        }

        thread::spawn(move || {
            eventloop.start().unwrap();
        });

        clients
    }
}
