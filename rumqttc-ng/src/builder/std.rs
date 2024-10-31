
use std::thread;

use client::blocking::Client;
use eventloop::EventLoop;
pub use eventloop::EventLoopSettings;
pub use transport::TransportSettings;

use crate::{client, eventloop, transport};

pub struct Builder {
    eventloop_settings: EventLoopSettings,
    transport_settings: TransportSettings,
    clients: Vec<Client>,
}


impl Builder {
    pub fn new() -> Self {
        Builder {
            eventloop_settings: EventLoopSettings::default(),
            transport_settings: TransportSettings::default(),
            clients: vec![],
        }
    }

    pub fn register_client(self, id: usize, size: usize) -> Self {
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
        let eventloop = EventLoop::new(self.eventloop_settings);
        thread::spawn(move || {
            eventloop.start();
        });

        self.clients
    }
}