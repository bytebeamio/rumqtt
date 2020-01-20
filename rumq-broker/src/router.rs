use derive_more::From;
use tokio::sync::mpsc::{self, Receiver, Sender};
use rumq_core::{Publish, Subscribe};

use std::collections::HashMap;

#[derive(Debug, From)]
pub enum Error {
    AllSendersDown,
    Mpsc(mpsc::error::SendError<RouterMessage>),
}

/// Router message to orchestrate data between connections. We can also
/// use this to send control signals to connections to modify their behavior
/// dynamically from the console
#[derive(Debug)]
pub enum RouterMessage {
    /// Client id and connection handle
    Connect((String, Sender<RouterMessage>)),
    /// Publish message to forward to connections
    Publish(Publish),
    /// Client id and subscription
    Subscribe((String, Subscribe))
}

pub struct Router {
    // handles to all connections. used to route data
    connections:   HashMap<String, Sender<RouterMessage>>,
    // maps subscription to interested clients. wildcards
    // aren't supported
    subscriptions: HashMap<String, Vec<String>>,
    // channel receiver to receive data from all the connections.
    // each connection will have a tx handle
    data_rx:       Receiver<RouterMessage>,
}

impl Router {
    pub fn new(data_rx: Receiver<RouterMessage>) -> Self {
        Router { connections: HashMap::new(), subscriptions: HashMap::new(), data_rx }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let message = match self.data_rx.recv().await {
                Some(message) => message,
                None => return Err(Error::AllSendersDown),
            };

            if let Err(err) = self.handle_router_message(message).await {
                error!("Handle packet error = {:?}", err);
            }
        }
    }

    async fn handle_router_message(&mut self, message: RouterMessage) -> Result<(), Error> {
        
        match message {
            RouterMessage::Connect((id, connection_handle)) => self.handle_connect(id, connection_handle)?,
            RouterMessage::Publish(publish) => self.handle_publish(publish).await?,
            RouterMessage::Subscribe((id, subscribe)) => self.handle_subscribe(id, subscribe)?,
        }

        Ok(())
    }

    fn handle_connect(&mut self, id: String, connection_handle: Sender<RouterMessage>) -> Result<(), Error> {
        debug!("Connect. Id = {:?}", id);
        self.connections.insert(id, connection_handle);
        Ok(())
    }

    async fn handle_publish(&mut self, publish: Publish) -> Result<(), Error> {
        debug!("Publish. Topic = {:?}, Qos = {:?}, Payload len = {}", publish.topic_name(), publish.qos(), publish.payload().len());
        
        // TODO: Will direct member access perform better than method call at higher frequency?
        let topic = publish.topic_name();

        // TODO: Directly get connection handles instead of client ids?
        if let Some(ids) = self.subscriptions.get(topic) {
            for id in ids.iter() {
                let connection = self.connections.get_mut(id).unwrap();
                let message = RouterMessage::Publish(publish.clone());
                connection.send(message).await?;
            }
        }

        Ok(())
    }

    fn handle_subscribe(&mut self, id: String, subscribe: Subscribe) -> Result<(), Error> {
        debug!("Subscribe. Id = {:?},  Topics = {:?}", id, subscribe.topics());
        
        // Each subscribe message can send multiple topics to subscribe to
        for topic in subscribe.topics() {
            let id = id.clone();
            match self.subscriptions.get_mut(topic.topic_path()) {
                // push client id to list of clients intrested in this subspcription
                Some(connections) => connections.push(id),
                // create a new subscription and push the client id
                None => {
                    let mut connections = Vec::new();
                    connections.push(id);
                    self.subscriptions.insert(topic.topic_path().to_owned(), connections);
                }
            }
        }

        // TODO: Handle duplicate subscriptions from the same client
        Ok(())
    }
}
