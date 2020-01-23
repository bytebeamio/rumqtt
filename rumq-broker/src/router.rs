use derive_more::From;
use rumq_core::{has_wildcards, matches, LastWill, Connect, Publish, Subscribe};
use tokio::sync::mpsc::{self, Receiver, Sender};

use std::collections::HashMap;
use std::mem;

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
    Connect((Connect, Sender<RouterMessage>)),
    /// Publish message to forward to connections
    Publish(Publish),
    /// Client id and subscription
    Subscribe((String, Subscribe)),
    /// Disconnects a client from active connections list. No will handling
    Disconnect(String),
    /// Disconnects a client from active connections list. Will handling
    Death(String),
}

pub struct Router {
    // handles to all active connections. used to route data
    active_connections:     HashMap<String, Sender<RouterMessage>>,
    // inactive persistent connections
    inactive_connections:   HashMap<String, Vec<rumq_core::Publish>>,
    // connection will
    connections_will:       HashMap<String, LastWill>,
    // maps concrete subscriptions to interested clients
    concrete_subscriptions: HashMap<String, Vec<String>>,
    // maps wildcard subscriptions to interested clients
    wild_subscriptions:     HashMap<String, Vec<String>>,
    // retained publishes
    retained_publishes:     HashMap<String, Publish>,
    // channel receiver to receive data from all the active_connections.
    // each connection will have a tx handle
    data_rx:                Receiver<RouterMessage>,
}

impl Router {
    pub fn new(data_rx: Receiver<RouterMessage>) -> Self {
        Router {
            active_connections: HashMap::new(),
            inactive_connections: HashMap::new(),
            connections_will: HashMap::new(),
            concrete_subscriptions: HashMap::new(),
            wild_subscriptions: HashMap::new(),
            retained_publishes: HashMap::new(),
            data_rx,
        }
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
            RouterMessage::Connect((connect, connection_handle)) => {
                self.handle_connect(connect, connection_handle)?
            }
            RouterMessage::Publish(publish) => self.handle_publish(publish).await?,
            RouterMessage::Subscribe((id, subscribe)) => self.handle_subscribe(id, subscribe).await?,
            RouterMessage::Disconnect(id) => self.handle_disconnection(id)?,
            RouterMessage::Death(id) => self.handle_death(id).await?,
        }

        Ok(())
    }

    fn handle_connect(&mut self, connect: Connect, connection_handle: Sender<RouterMessage>) -> Result<(), Error> {
        let id = connect.client_id;
        let clean_session = connect.clean_session;
        
        debug!("Connect. Id = {:?}", id);
        self.active_connections.insert(id.clone(), connection_handle);

        if clean_session {
            // FIXME: This is costly for every clean connection with a lot of subscriptions
            for (_, ids) in self.concrete_subscriptions.iter_mut() {
                if let Some(index) = ids.iter().position(|r| r == &id) {
                    ids.remove(index);
                }
            }

            for (_, ids) in self.wild_subscriptions.iter_mut() {
                if let Some(index) = ids.iter().position(|r| r == &id) {
                    ids.remove(index);
                }
            }
        }

        if let Some(will) = connect.last_will {
            self.connections_will.insert(id, will);
        }
        Ok(())
    }

    async fn handle_publish(&mut self, publish: Publish) -> Result<(), Error> {
        debug!(
            "Publish. Topic = {:?}, Qos = {:?}, Payload len = {}",
            publish.topic_name(),
            publish.qos(),
            publish.payload().len()
        );

        if publish.retain {
            if publish.payload.len() == 0 {
                self.retained_publishes.remove(&publish.topic_name);
                return Ok(());
            } else {
                self.retained_publishes.insert(publish.topic_name.clone(), publish.clone());
            }
        }

        // TODO: Will direct member access perform better than method call at higher frequency?
        // TODO: Directly get connection handles instead of client ids?
        let topic = publish.topic_name();
        if let Some(ids) = self.concrete_subscriptions.get(topic) {
            for id in ids.iter() {
                if let Some(connection) = self.active_connections.get_mut(id) {
                    let message = RouterMessage::Publish(publish.clone());
                    if let Err(e) = connection.send(message).await {
                        error!("1 Forward failed. Error = {:?}", e);
                    }
                }

                // TODO: Offline message handling
            }
        }

        // TODO: O(n) which happens during every publish. publish perf is going to be
        // linearly degraded based on number of wildcard subscriptions. fix this
        for (filter, ids) in self.wild_subscriptions.iter() {
            if matches(topic, filter) {
                for id in ids.iter() {
                    if let Some(connection) = self.active_connections.get_mut(id) {
                        let message = RouterMessage::Publish(publish.clone());
                        if let Err(e) = connection.send(message).await {
                            error!("2 Forward failed. Error = {:?}", e);
                        }
                    }

                    // TODO: Offline message handling
                }
            }
        }

        Ok(())
    }

    async fn handle_subscribe(&mut self, id: String, subscribe: Subscribe) -> Result<(), Error> {
        debug!("Subscribe. Id = {:?},  Topics = {:?}", id, subscribe.topics());

        // Each subscribe message can send multiple topics to subscribe to. handle dupicates
        for topic in subscribe.topics() {
            let id = id.clone();
            let filter = topic.topic_path();

            // client subscribing to a/b/c and a/+/c should receive message only once when
            // a publish happens on a/b/c.
            let subscriptions = if has_wildcards(filter) {
                // remove client from the concrete subscription list incase of a matching wildcard
                // subscription
                for (topic, clients) in self.concrete_subscriptions.iter_mut() {
                    if matches(topic, filter) {
                        if let Some(index) = clients.iter().position(|r| r == &id) {
                            clients.remove(index);
                        }
                    }
                }

                &mut self.wild_subscriptions
            } else {
                // ignore a new concrete subscription if the client already has a matching wildcard
                // subscription
                for (topic, clients) in self.concrete_subscriptions.iter_mut() {
                    if matches(topic, filter) {
                        if let Some(_) = clients.iter().position(|r| r == &id) {
                            return Ok(());
                        }
                    }
                }

                &mut self.concrete_subscriptions
            };

            // add client id to subscriptions
            match subscriptions.get_mut(filter) {
                // push client id to the list of clients intrested in this subspcription
                Some(connections) => {
                    // don't add the same id twice
                    if !connections.contains(&id) {
                        connections.push(id.clone())
                    }
                }
                // create a new subscription and push the client id
                None => {
                    let mut connections = Vec::new();
                    connections.push(id.clone());
                    subscriptions.insert(filter.to_owned(), connections);
                }
            }

            // Handle retained publishes after subscription duplicates are handled above
            if has_wildcards(filter) {
                for (topic, publish) in self.retained_publishes.iter() {
                    if matches(topic, filter) {
                        if let Some(connection) = self.active_connections.get_mut(&id) {
                            connection.send(RouterMessage::Publish(publish.clone())).await?;
                        }
                    }
                }
            } else {
                if let Some(publish) = self.retained_publishes.get(filter) {
                    if let Some(connection) = self.active_connections.get_mut(&id) {
                        connection.send(RouterMessage::Publish(publish.clone())).await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_disconnection(&mut self, id: String) -> Result<(), Error> {
        self.connections_will.remove(&id);

        if let Some(_) = self.active_connections.remove(&id) {
            self.inactive_connections.insert(id, Vec::new());
        }

        Ok(())
    }

    async fn handle_death(&mut self, id: String) -> Result<(), Error> {
        if let Some(_) = self.active_connections.remove(&id) {
            self.inactive_connections.insert(id.clone(), Vec::new());
        }

        if let Some(mut will) = self.connections_will.remove(&id) {
            let topic = mem::replace(&mut will.topic, "".to_owned());
            let message = mem::replace(&mut will.message, "".to_owned());
            let qos = will.qos;

            let mut publish = rumq_core::publish(topic, message);
            publish.set_qos(qos);

            self.handle_publish(publish).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn persistent_disconnected_and_dead_connections_are_moved_to_inactive_state() {}

    #[test]
    fn persistend_reconnections_are_move_from_inactive_to_active_state() {}

    #[test]
    fn offline_messages_are_given_back_to_reconnected_persistent_connection() {}

    #[test]
    fn remove_client_from_concrete_subsctiptions_if_new_wildcard_subscription_matches_existing_concrecte_subscription() {
        // client subscibing to a/b/c and a/+/c should receive message only once when
        // a publish happens on a/b/c
    }

    #[test]
    fn ingnore_new_concrete_subscription_if_a_matching_wildcard_subscription_exists_for_the_client() {}

    #[test]
    fn router_should_remove_the_connection_during_disconnect() {}

    #[test]
    fn router_should_not_add_same_client_to_subscription_list() {}

    #[test]
    fn router_saves_offline_messages_of_a_persistent_dead_connection() {}
}
