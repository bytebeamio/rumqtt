use derive_more::From;
use rumq_core::{has_wildcards, matches, QoS, Packet, Connect, Publish, Subscribe, Unsubscribe};
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

#[derive(Debug, Clone)]
struct Subscriber {
    client_id: String,
    qos: QoS,
}

impl Subscriber {
    pub fn new(id: &str, qos: QoS) -> Subscriber {
        Subscriber {
            client_id: id.to_owned(),
            qos,
        }
    }
}

pub struct Router {
    // handles to all active connections. used to route data
    active_connections:     HashMap<String, ActiveConnection>,
    // inactive persistent connections
    inactive_connections:   HashMap<String, InactiveConnection>,
    // maps concrete subscriptions to interested subscribers
    concrete_subscriptions: HashMap<String, Vec<Subscriber>>,
    // maps wildcard subscriptions to interested subscribers
    wild_subscriptions:     HashMap<String, Vec<Subscriber>>,
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
                    Packet::Subscribe(subscribe) => self.add_to_subscriptions(id, subscribe),
                    Packet::Unsubscribe(unsubscribe) => self.remove_from_subscriptions(id, unsubscribe),
                    Packet::Disconnect => self.deactivate(id),
                    _ => return Ok(()),
                }
            }
            RouterMessage::Death(id) => self.deactivate_and_forward_will(id),
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
            // FIXME: This is costly for every clean connection with a lot of subscribers
            for (_, subscribers) in self.concrete_subscriptions.iter_mut() {
                if let Some(index) = subscribers.iter().position(|s| s.client_id == id) {
                    subscribers.remove(index);
                }
            }

            for (_, subscribers) in self.wild_subscriptions.iter_mut() {
                if let Some(index) = subscribers.iter().position(|s| s.client_id == id) {
                    subscribers.remove(index);
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
        if let Some(subscribers) = self.concrete_subscriptions.get(topic) {
            for subscriber in subscribers.iter() {
                forward_publish(subscriber, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
            }
        }

        // TODO: O(n) which happens during every publish. publish perf is going to be
        // linearly degraded based on number of wildcard subscriptions. fix this
        for (filter, subscribers) in self.wild_subscriptions.iter() {
            if matches(topic, filter) {
                for subscriber in subscribers.iter() {
                    forward_publish(subscriber, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
                }
            }
        };
    }


    fn add_to_subscriptions(&mut self, id: String, subscribe: Subscribe) {
        debug!("Subscribe. Id = {:?},  Topics = {:?}", id, subscribe.topics());

        // Each subscribe message can send multiple topics to subscribe to. handle dupicates
        for topic in subscribe.topics() {
            let mut filter = topic.topic_path().clone();
            let qos = topic.qos;
            let subscriber = Subscriber::new(&id, qos);

            let subscriber = if let Some((f, subscriber)) = self.fix_overlapping_subscriptions(&id, &filter, qos) {
                filter = f;
                subscriber
            } else {
                subscriber
            };

            // a publish happens on a/b/c.
            let subscriptions = if has_wildcards(&filter) {
                &mut self.wild_subscriptions
            } else {
                &mut self.concrete_subscriptions
            };

            // add client id to subscriptions
            match subscriptions.get_mut(&filter) {
                // push client id to the list of clients intrested in this subspcription
                Some(subscribers) => {
                    // don't add the same id twice
                    if !subscribers.iter().any(|s| s.client_id == id) {
                        subscribers.push(subscriber.clone())
                    }
                }
                // create a new subscription and push the client id
                None => {
                    let mut subscribers = Vec::new();
                    subscribers.push(subscriber.clone());
                    subscriptions.insert(filter.to_owned(), subscribers);
                }
            }

            // Handle retained publishes after subscription duplicates are handled above
            if has_wildcards(&filter) {
                for (topic, publish) in self.retained_publishes.iter() {
                    if matches(topic, &filter) {
                        forward_publish(&subscriber, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
                    }
                }
            } else {
                if let Some(publish) = self.retained_publishes.get(&filter) {
                    forward_publish(&subscriber, publish.clone(), &mut self.active_connections, &mut self.inactive_connections);
                }
            }
        }
    }

    /// removes the subscriber from subscription if the current subscription is wider than the
    /// existing subscription and returns it
    ///
    /// if wildcard subscription:
    /// move subscriber from concrete to wild subscription with greater qos
    /// move subscriber from existing wild to current wild subscription if current is wider
    /// move subscriber from current wild to existing wild if the existing wild is wider
    /// returns the subscriber and the wild subscription it is to be added to
    /// none implies that there are no overlapping subscriptions for this subscriber
    /// new subscriber a/+/c (qos 1) matches existing subscription a/b/c
    /// subscriber should be moved from a/b/c to a/+/c
    ///
    /// * if the new subscription is wider than existing subscription, move the subscriber to wider
    /// subscription with highest qos
    ///
    /// * any new wildcard subsciption checks for matching concrete subscriber
    /// * if matches, add the subscriber to `wild_subscriptions` with greatest qos 
    ///
    /// * any new concrete subscriber checks for matching wildcard subscriber
    /// * if matches, add the subscriber to `wild_subscriptions` (instead of concrete subscription) with greatest qos
    /// 
    /// coming to overlapping wildcard subscriptions
    ///
    /// * new subsciber-a a/+/c/d  mathes subscriber-a in a/# 
    /// * add subscriber-a to a/# directly with highest qos
    /// 
    /// * new subscriber a/# matches with existing a/+/c/d
    /// * remove subscriber from a/+/c/d and move it to a/# with highest qos
    /// 
    /// * finally a subscriber won't be part of multiple subscriptions
    fn fix_overlapping_subscriptions(&mut self, id: &str, current_filter: &str, qos: QoS) -> Option<(String, Subscriber)> {
        let mut subscriber = None;
        let mut filter = current_filter.to_owned();
        let mut qos = qos;

        // subscriber in concrete_subscriptions a/b/c/d matchs new subscription a/+/c/d on same
        // subscriber. move it from concrete to wild
        if has_wildcards(current_filter) {
            for (existing_filter, subscribers) in self.concrete_subscriptions.iter_mut() {
                if matches(existing_filter, current_filter) {
                    if let Some(index) = subscribers.iter().position(|s| s.client_id == id) {
                        let mut s = subscribers.remove(index);
                        if qos > s.qos {
                            s.qos = qos;
                        }
                        subscriber = Some(s);
                    }
                }
            }

            for (existing_filter, subscribers) in self.wild_subscriptions.iter_mut() {
                // current filter is wider than existing filter. remove subscriber (if it exists)
                // from current filter
                if matches(existing_filter, current_filter) {
                    if let Some(index) = subscribers.iter().position(|s| s.client_id == id) {
                        let s = subscribers.remove(index);

                        if s.qos > qos {
                            qos = s.qos
                        }

                        subscriber = Some(s);
                    }
                } else if matches(current_filter, existing_filter) {
                    // existing filter is wider than current filter, return the subscriber with
                    // wider subscription (to be added outside this method)
                    filter = existing_filter.clone();
                    // remove the subscriber and update the global qos (this subscriber will be
                    // added again outside)
                    if let Some(index) = subscribers.iter().position(|s| s.client_id == id) {
                        let s = subscribers.remove(index);
                        
                        if s.qos > qos {
                            qos = s.qos
                        }

                        subscriber = Some(s);
                    }   
                }
            }
        } 

        match subscriber {
            Some(mut subscriber) => {
                subscriber.qos = qos;
                Some((filter, subscriber))
            }
            None => None
        }
    }

    fn remove_from_subscriptions(&mut self, id: String, unsubscribe: Unsubscribe) {
        for topic in unsubscribe.topics.iter() {
            if has_wildcards(topic) {
                // remove client from the concrete subscription list incase of a matching wildcard
                // subscription
                for (filter, subscribers) in self.concrete_subscriptions.iter_mut() {
                    if topic == filter {
                        if let Some(index) = subscribers.iter().position(|s| s.client_id == id) {
                            subscribers.remove(index);
                        }
                    }
                }
            } else {
                // ignore a new concrete subscription if the client already has a matching wildcard
                // subscription
                for (filter, subscribers) in self.concrete_subscriptions.iter_mut() {
                    if topic == filter {
                        if let Some(index) = subscribers.iter().position(|s| s.client_id == id) {
                            subscribers.remove(index);
                        }
                    }
                }
            };
        }
    }

    fn deactivate(&mut self, id: String) {
        info!("Deactivating client due to disconnect packet. Id = {}", id);

        if let Some(connection) = self.active_connections.remove(&id) {
            if !connection.state.clean_session {
                self.inactive_connections.insert(id, InactiveConnection::new(connection.state));
            }
        }
    }

    fn deactivate_and_forward_will(&mut self, id: String) {
        info!("Deactivating client due to connection death. Id = {}", id);

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
    }
}


// forwards data to the connection with the following id
fn forward_publish(subscriber: &Subscriber, mut publish: Publish, active_connections: &mut HashMap<String, ActiveConnection>, inactive_connections: &mut HashMap<String, InactiveConnection>)  {
    publish.qos = subscriber.qos;

    if let Some(connection) = inactive_connections.get_mut(&subscriber.client_id) {
        debug!("Forwarding publish to inactive connection. Id = {}", subscriber.client_id);
        connection.state.handle_outgoing_publish(publish); 
        return
    }

    debug!("Forwarding publish to an active connection. Id = {}", subscriber.client_id);
    if let Some(connection) = active_connections.get_mut(&subscriber.client_id) {
        let packet = connection.state.handle_outgoing_publish(publish); 
        let message = RouterMessage::Packet(packet);

        // slow connections should be moved to inactive connections. This drops tx handle of the
        // connection leading to connection disconnection
        if let Err(e) = connection.tx.try_send(message) {
            match e {
                TrySendError::Full(_m) => {
                    error!("Slow connection. Dropping handle and moving id to inactive list");
                    if let Some(connection) = active_connections.remove(&subscriber.client_id) {
                        inactive_connections.insert(subscriber.client_id.clone(), InactiveConnection::new(connection.state));
                    }
                }
                TrySendError::Closed(_m) => {
                    error!("Closed connection. Forward failed");
                    active_connections.remove(&subscriber.client_id);
                }
            }
        }
    }
}

/// Saves state and sends network reply back to the connection
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
