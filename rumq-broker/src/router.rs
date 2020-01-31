use derive_more::From;
use rumq_core::{has_wildcards, matches, Packet, Connect, Publish, Subscribe};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TrySendError;

use std::collections::{HashMap, VecDeque};
use std::mem;

use crate::state::MqttState;

#[derive(Debug, From)]
pub enum Error {
    State,
    AllSendersDown,
    Mpsc(TrySendError<RouterMessage>),
}

/// Router message to orchestrate data between connections. We can also
/// use this to send control signals to connections to modify their behavior
/// dynamically from the console
#[derive(Debug)]
pub enum RouterMessage {
    /// Client id and connection handle
    Connect((Connect, Sender<RouterMessage>)),
    /// Packet
    Packet(rumq_core::Packet),
    /// Disconnects a client from active connections list. Will handling
    Death(String),
    /// Pending messages of the previous connection
    Pending(Option<VecDeque<Publish>>)
}


#[derive(Debug)]
struct ActiveConnection {
    pub state: MqttState,
    tx: Sender<RouterMessage>
}

impl ActiveConnection {
    pub fn new(tx: Sender<RouterMessage>, state: MqttState) -> ActiveConnection {
        ActiveConnection {
            state,
            tx
        }
    }
}

#[derive(Debug)]
struct InactiveConnection {
    pub state: MqttState
}

impl InactiveConnection {
    pub fn new(state: MqttState) -> InactiveConnection {
        InactiveConnection {
            state,
        }
    }
}

pub struct Router {
    // handles to all active connections. used to route data
    active_connections:     HashMap<String, ActiveConnection>,
    // inactive persistent connections
    inactive_connections:   HashMap<String, InactiveConnection>,
    // maps concrete subscriptions to interested clients
    concrete_subscriptions: HashMap<String, Vec<String>>,
    // maps wildcard subscriptions to interested clients
    wild_subscriptions:     HashMap<String, Vec<String>>,
    // retained publishes
    retained_publishes:     HashMap<String, Publish>,
    // channel receiver to receive data from all the active_connections.
    // each connection will have a tx handle
    data_rx:                Receiver<(String, RouterMessage)>,
}

impl Router {
    pub fn new(data_rx: Receiver<(String, RouterMessage)>) -> Self {
        Router {
            active_connections: HashMap::new(),
            inactive_connections: HashMap::new(),
            concrete_subscriptions: HashMap::new(),
            wild_subscriptions: HashMap::new(),
            retained_publishes: HashMap::new(),
            data_rx,
        }
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        loop {
            let (id, message) = match self.data_rx.recv().await {
                Some(message) => message,
                None => return Err(Error::AllSendersDown),
            };

            if let Err(err) = self.handle_incoming_router_message(id, message).await {
                error!("Handle packet error = {:?}", err);
            }
        }
    }

    async fn handle_incoming_router_message(&mut self, id: String, message: RouterMessage) -> Result<(), Error> {
        match message {
            RouterMessage::Connect((connect, connection_handle)) => {
                self.handle_connect(connect, connection_handle)?
            }
            RouterMessage::Packet(packet) => {
                handle_incoming_packet(&id, packet.clone(), &mut self.active_connections)?;
                match packet {
                    Packet::Publish(publish) => self.match_subscriptions_and_forward(publish),
                    Packet::Subscribe(subscribe) => self.add_to_subscriptions(id, subscribe).await?,
                    Packet::Disconnect => self.deactivate(id)?,
                    _ => return Ok(()),
                }
            }
            RouterMessage::Death(id) => self.deactivate_and_forward_will(id).await?,
            RouterMessage::Pending(_) => return Ok(())
        }

        Ok(())
    }

    fn handle_connect(&mut self, connect: Connect, mut connection_handle: Sender<RouterMessage>) -> Result<(), Error> {
        let id = connect.client_id;
        let clean_session = connect.clean_session;
        let will = connect.last_will;

        debug!("Connect. Id = {:?}", id);

        if clean_session {
            self.inactive_connections.remove(&id);
            
            let state = MqttState::new(clean_session, will);
            connection_handle.try_send(RouterMessage::Pending(None))?;
            self.active_connections.insert(id.clone(), ActiveConnection::new(connection_handle, state));
        } else {
            if let Some(connection) = self.inactive_connections.remove(&id) {
                connection_handle.try_send(RouterMessage::Pending(Some(connection.state.outgoing_publishes.clone())))?;
                self.active_connections.insert(id.clone(), ActiveConnection::new(connection_handle, connection.state));
            } else {
                let state = MqttState::new(clean_session, will);
                connection_handle.try_send(RouterMessage::Pending(None))?;
                self.active_connections.insert(id.clone(), ActiveConnection::new(connection_handle, state));
            }
        }

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

        Ok(())
    }

    fn match_subscriptions_and_forward(&mut self,  publish: Publish) {
        debug!(
            "Publish. Topic = {:?}, Qos = {:?}, Payload len = {}",
            publish.topic_name(),
            publish.qos(),
            publish.payload().len()
        );

        if publish.retain {
            if publish.payload.len() == 0 {
                self.retained_publishes.remove(&publish.topic_name);
                return
            } else {
                self.retained_publishes.insert(publish.topic_name.clone(), publish.clone());
            }
        }

        let topic = publish.topic_name();
        if let Some(ids) = self.concrete_subscriptions.get(topic) {
            for id in ids.iter() {
                forward_publish(id, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
            }
        }

        // TODO: O(n) which happens during every publish. publish perf is going to be
        // linearly degraded based on number of wildcard subscriptions. fix this
        for (filter, ids) in self.wild_subscriptions.iter() {
            if matches(topic, filter) {
                for id in ids.iter() {
                    forward_publish(id, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
                }
            }
        };
    }


    async fn add_to_subscriptions(&mut self, id: String, subscribe: Subscribe) -> Result<(), Error> {
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
                        forward_publish(&id, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
                    }
                }
            } else {
                if let Some(publish) = self.retained_publishes.get(filter) {
                    forward_publish(&id, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
                }
            }
        }

        Ok(())
    }

    fn deactivate(&mut self, id: String) -> Result<(), Error> {
        info!("Deactivating client due to disconnect packet");

        if let Some(connection) = self.active_connections.remove(&id) {
            if !connection.state.clean_session {
                self.inactive_connections.insert(id, InactiveConnection::new(connection.state));
            }
        }

        Ok(())
    }

    async fn deactivate_and_forward_will(&mut self, id: String) -> Result<(), Error> {
        info!("Deactivating client due to connection death");
        
        if let Some(mut connection) = self.active_connections.remove(&id) {
            if let Some(mut will) = connection.state.will.take() {
                let topic = mem::replace(&mut will.topic, "".to_owned());
                let message = mem::replace(&mut will.message, "".to_owned());
                let qos = will.qos;

                let mut publish = rumq_core::publish(topic, message);
                publish.set_qos(qos);

                self.match_subscriptions_and_forward(publish);
            }

            if !connection.state.clean_session {
                self.inactive_connections.insert(id.clone(), InactiveConnection::new(connection.state));
            }
        }


        Ok(())
    }
}

// forwards data to the connection with the following id
fn forward_publish(id: &str, publish: Publish, active_connections: &mut HashMap<String, ActiveConnection>, inactive_connections: &mut HashMap<String, InactiveConnection>)  {
    if let Some(connection) = inactive_connections.get_mut(id) {
        debug!("Forwarding publish to inactive connection. Id = {}", id);
        connection.state.handle_outgoing_publish(publish); 
        return
    }

    debug!("Forwarding publish to an active connection. Id = {}", id);
    if let Some(connection) = active_connections.get_mut(id) {
        let packet = connection.state.handle_outgoing_publish(publish); 
        let message = RouterMessage::Packet(packet);


        // slow connections should be moved to inactive connections. This drops tx handle of the
        // connection leading to connection disconnection
        if let Err(e) = connection.tx.try_send(message) {
            match e {
                TrySendError::Full(_m) => {
                    error!("Slow connection. Dropping handle and moving id to inactive list");
                    if let Some(connection) = active_connections.remove(id) {
                        inactive_connections.insert(id.to_owned(), InactiveConnection::new(connection.state));
                    }
                }
                TrySendError::Closed(_m) => {
                    error!("Closed connection. Forward failed");
                    active_connections.remove(id);
                }
            }
        }
    }
}

fn handle_incoming_packet(id: &str, packet: Packet, active_connections: &mut HashMap<String, ActiveConnection>) -> Result<(), Error> {
    if let Some(connection) = active_connections.get_mut(id) {
        let reply = match connection.state.handle_incoming_mqtt_packet(packet) {
            Ok(Some(reply)) => reply,
            Ok(None) => return Ok(()),
            Err(e) => {
                error!("State error = {:?}", e);
                active_connections.remove(id);
                return Err(Error::State)
            }
        };

        if let Err(e) = connection.tx.try_send(reply) {
            match e {
                TrySendError::Full(_m) => {
                    error!("Slow connection. Dropping handle and moving id to inactive list");
                    active_connections.remove(id);
                }
                TrySendError::Closed(_m) => {
                    error!("Closed connection. Forward failed");
                    active_connections.remove(id);
                }
            }
        }
    }

    Ok(())
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
