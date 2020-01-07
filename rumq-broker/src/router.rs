use tokio::sync::mpsc::{Receiver, Sender};
use derive_more::From;

use rumq_core::{Packet, Connect, Publish, Subscribe};

use std::collections::HashMap;

use crate::graveyard::Graveyard;

#[derive(Debug, From)]
pub enum Error {
    AllSendersDown,
    PacketNotSupported(Packet)
}

pub struct Router {
    graveyard: Graveyard,
    // handles to all connections. used to route data
    connections: HashMap<String, Sender<Packet>>,
    // maps subscription to interested clients. wildcards
    // aren't supported
    subscriptions: HashMap<String, Vec<String>>,
    // channel receiver to receive data from all the connections.
    // each connection will have a tx handle
    data_rx: Receiver<Packet>,
}

impl Router {
    pub fn new(graveyard: Graveyard, data_rx: Receiver<Packet>) -> Self {
        Router { graveyard, connections: HashMap::new(), subscriptions: HashMap::new(), data_rx }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let packet = match self.data_rx.recv().await {
                Some(packet) => packet,
                None => return Err(Error::AllSendersDown)
            };

            if let Err(err) = self.handle_packet(packet) {
                error!("Handle packet error = {:?}", err);
            }
        }
    }

    fn handle_packet(&mut self, packet: Packet) -> Result<(), Error> {
        match packet {
            Packet::Connect(connect) => self.handle_connect(connect)?,
            Packet::Publish(publish) => self.handle_publish(publish)?,
            Packet::Subscribe(subscribe) => self.handle_subscribe(subscribe)?,
            packet => return Err(Error::PacketNotSupported(packet))
        }

        Ok(())
    }

    fn handle_connect(&mut self, connect: Connect) -> Result<(), Error> {
        let id = connect.client_id();
        let connection_handle = self.graveyard.connection_handle(&id);
        self.connections.insert(id.clone(), connection_handle);
        Ok(())
    }

    fn handle_publish(&mut self, _publish: Publish) -> Result<(), Error> {
        unimplemented!();
    }

    fn handle_subscribe(&mut self, subscribe: Subscribe) -> Result<(), Error> {
        let id = "dummy";
        // TODO: Handle duplicate subscriptions from the same client
        for topic in subscribe.topics() {
            if let Some(connections) = self.subscriptions.get_mut(topic.topic_path()) {
                connections.push(id.to_owned());
            } else {
                let connections = vec![id.to_owned()];
                self.subscriptions.insert(topic.topic_path().to_owned(), connections);
            };
        }

        Ok(())
    }
}
