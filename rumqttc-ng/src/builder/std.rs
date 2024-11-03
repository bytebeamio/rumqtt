use std::{collections::VecDeque, thread};

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
        let mut events_txs = VecDeque::new();
        for id in 0..self.eventloop_settings.max_clients {
            events_txs.push_back(events.producer(id));
        }

        let mut eventloop = EventLoop::new(self.eventloop_settings, events);

        // Create clients
        let mut clients = Vec::with_capacity(self.clients.len());
        for (id, size) in self.clients {
            let (a, b) = pipe(id, size, size);

            let tx = Tx {
                events_tx: events_txs.pop_front().unwrap(),
                tx: a,
            };

            eventloop.register_client(b);
            clients.push(Client::new(id, tx));
        }

        // Register network
        let (a, b) = pipe(0, 1024, 1024);
        eventloop.register_network(b);

        thread::spawn(move || {
            eventloop.start().unwrap();
        });

        thread::spawn(move || {
            let mut network = a;
            loop {
                network.incoming_recycler.recv();
            }
        });

        clients
    }
}
