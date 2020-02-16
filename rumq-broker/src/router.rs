use derive_more::From;
use rumq_core::mqtt4::{has_wildcards, matches, publish, QoS, Packet, Connect, Publish, Subscribe, Unsubscribe};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TrySendError;
use tokio::select;
use tokio::time::{self, Duration};
use tokio::stream::StreamExt;

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::fmt;

use crate::state::{self, MqttState};

#[derive(Debug, From)]
pub enum Error {
    State(state::Error),
    AllSendersDown,
    Mpsc(TrySendError<RouterMessage>),
}

/// Router message to orchestrate data between connections. We can also
/// use this to send control signals to connections to modify their behavior
/// dynamically from the console
#[derive(Debug)]
pub enum RouterMessage {
    /// Client id and connection handle
    Connect(Connection),
    /// Packet
    Packet(Packet),
    /// Packets
    Packets(VecDeque<Packet>),
    /// Disconnects a client from active connections list. Will handling
    Death(String),
    /// Pending messages of the previous connection
    Pending(VecDeque<Publish>)
}

pub struct Connection {
    pub connect: Connect,
    pub handle: Option<Sender<RouterMessage>>
}

impl Connection {
    pub fn new(connect: Connect, handle: Sender<RouterMessage>) -> Connection {
        Connection {
            connect,
            handle: Some(handle)
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.connect)
    }
}

#[derive(Debug)]
struct ActiveConnection {
    pub state: MqttState,
    pub outgoing: VecDeque<Packet>,
    tx: Sender<RouterMessage>
}

impl ActiveConnection {
    pub fn new(tx: Sender<RouterMessage>, state: MqttState) -> ActiveConnection {
        ActiveConnection {
            state,
            outgoing: VecDeque::new(),
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

#[derive(Debug)]
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
        let mut interval = time::interval(Duration::from_millis(10));
        
        loop {
            select! {
                o = self.data_rx.recv() => {
                    let (id, mut message) = o.unwrap();                   
                    debug!("In router message. Id = {}, {:?}", id, message);
                    match self.reply(id.clone(), &mut message) {
                        Ok(Some(message)) => self.forward(&id, message),
                        Ok(None) => (),
                        Err(e) => {
                            error!("Incoming handle error = {:?}", e);
                            continue;
                        }
                    }

                    // adds routes, routes the message to other connections etc etc
                    if let Err(e) = self.route(id, message) {
                        error!("Routing error = {:?}", e);
                    }
                }
                _ = interval.next() => {
                    for (_, connection) in self.active_connections.iter_mut() {
                        let pending = connection.outgoing.split_off(0);
                        if pending.len() > 0 {
                            let _ = connection.tx.try_send(RouterMessage::Packets(pending));
                        }
                    }
                }
            }


        }
    }

    /// replys back to the connection sending the message
    /// doesn't modify any routing information of the router
    /// all the routing and route modifications due to subscriptions
    /// are part of `route` method
    /// generates reply to send backto the connection. Shouldn't touch anything except active and 
    /// inactive connections
    /// No routing modifications here
    fn reply(&mut self, id: String, message: &mut RouterMessage) -> Result<Option<RouterMessage>, Error> {
        match message {
            RouterMessage::Connect(connection) => {
                let handle = connection.handle.take().unwrap();
                let message = self.handle_connect(connection.connect.clone(), handle)?;
                Ok(message)
            }
            RouterMessage::Packet(packet) => {
                let message = self.handle_incoming_packet(&id, packet.clone())?;
                Ok(message)
            }
            _ => Ok(None)
        }
    }

    fn route(&mut self, id: String, message: RouterMessage) -> Result<(), Error> {
        match message {
            RouterMessage::Packet(packet) => {
                match packet {
                    Packet::Publish(publish) => self.match_subscriptions(&id, publish),
                    Packet::Subscribe(subscribe) => self.add_to_subscriptions(id, subscribe),
                    Packet::Unsubscribe(unsubscribe) => self.remove_from_subscriptions(id, unsubscribe),
                    Packet::Disconnect => self.deactivate(id),
                    _ => return Ok(())
                }
            }
            RouterMessage::Death(id) => {
                self.deactivate_and_forward_will(id.to_owned());
            }
            _ => () 
        }
        Ok(())
    }

    fn handle_connect(&mut self, connect: Connect, connection_handle: Sender<RouterMessage>) -> Result<Option<RouterMessage>, Error> {
        let id = connect.client_id;
        let clean_session = connect.clean_session;
        let will = connect.last_will;

        info!("Connect. Id = {:?}", id);
        let reply = if clean_session {
            self.inactive_connections.remove(&id);

            let state = MqttState::new(clean_session, will);
            self.active_connections.insert(id.clone(), ActiveConnection::new(connection_handle, state));
            Some(RouterMessage::Pending(VecDeque::new()))
        } else {
            if let Some(connection) = self.inactive_connections.remove(&id) {
                let pending = connection.state.outgoing_publishes.clone();
                self.active_connections.insert(id.clone(), ActiveConnection::new(connection_handle, connection.state));
                Some(RouterMessage::Pending(pending))
            } else {
                let state = MqttState::new(clean_session, will);
                self.active_connections.insert(id.clone(), ActiveConnection::new(connection_handle, state));
                Some(RouterMessage::Pending(VecDeque::new()))
            }
        };

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

        Ok(reply)
    }

    fn match_subscriptions(&mut self, _id: &str, publish: Publish) {
        if publish.retain {
            if publish.payload.len() == 0 {
                self.retained_publishes.remove(&publish.topic_name);
                return
            } else {
                self.retained_publishes.insert(publish.topic_name.clone(), publish.clone());
            }
        }

        let topic = &publish.topic_name;
        if let Some(subscribers) = self.concrete_subscriptions.get(topic) {
            let subscribers = subscribers.clone();
            for subscriber in subscribers.iter() {
                self.fill_subscriber(subscriber, publish.clone());
            }
        }

        // TODO: O(n) which happens during every publish. publish perf is going to be
        // linearly degraded based on number of wildcard subscriptions. fix this
        let wild_subscriptions = self.wild_subscriptions.clone(); 
        for (filter, subscribers) in wild_subscriptions.into_iter() {
            if matches(&topic, &filter) {
                for subscriber in subscribers.into_iter() {
                    let publish = publish.clone();
                    self.fill_subscriber(&subscriber, publish);
                }
            }
        };
    }


    fn add_to_subscriptions(&mut self, id: String, subscribe: Subscribe) {
        // Each subscribe message can send multiple topics to subscribe to. handle dupicates
        for topic in subscribe.topics {
            let mut filter = topic.topic_path.clone();
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
                let retained_publishes = self.retained_publishes.clone();
                for (topic, publish) in retained_publishes.into_iter() {
                    if matches(&topic, &filter) {
                        self.fill_subscriber(&subscriber, publish);
                    }
                }
            } else {
                if let Some(publish) = self.retained_publishes.get(&filter) {
                    let publish = publish.clone();
                    self.fill_subscriber(&subscriber, publish);
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

                let publish = publish(topic, qos, message);
                self.match_subscriptions(&id, publish);
            }

            if !connection.state.clean_session {
                self.inactive_connections.insert(id.clone(), InactiveConnection::new(connection.state));
            }
        }
    }

    /// Saves state and sends network reply back to the connection
    fn handle_incoming_packet(&mut self, id: &str, packet: Packet) -> Result<Option<RouterMessage>, Error> {
        if let Some(connection) = self.active_connections.get_mut(id) {
            let reply = match connection.state.handle_incoming_mqtt_packet(packet) {
                Ok(Some(reply)) => reply,
                Ok(None) => return Ok(None),
                Err(state::Error::Unsolicited(packet)) => {
                    // NOTE: Some clients seems to be sending pending acks after reconnection
                    // even during a clean session. Let's be little lineant for now
                    error!("Unsolicited ack = {:?}. Id = {}", packet, id);
                    return Ok(None)
                }
                Err(e) => {
                    error!("State error = {:?}. Id = {}", e, id);
                    self.active_connections.remove(id);
                    return Err::<_, Error>(e.into())
                }
            };

            return Ok(Some(reply))
        }

        Ok(None)
    }

    // forwards data to the connection with the following id
    fn fill_subscriber(&mut self, subscriber: &Subscriber, mut publish: Publish)  {
        publish.qos = subscriber.qos;

        if let Some(connection) = self.inactive_connections.get_mut(&subscriber.client_id) {
            debug!("Forwarding publish to active connection. Id = {}, {:?}", subscriber.client_id, publish);
            connection.state.handle_outgoing_publish(publish); 
            return
        }

        if let Some(connection) = self.active_connections.get_mut(&subscriber.client_id) {
            let packet = connection.state.handle_outgoing_publish(publish); 
            connection.outgoing.push_back(packet);
        }
    }

    fn forward(&mut self, id: &str, message: RouterMessage) {
        if let Some(connection) = self.active_connections.get_mut(id) {
            // slow connections should be moved to inactive connections. This drops tx handle of the
            // connection leading to connection disconnection
            if let Err(e) = connection.tx.try_send(message) {
                match e {
                    TrySendError::Full(_m) => {
                        error!("Slow connection during forward. Dropping handle and moving id to inactive list. Id = {}", id);
                        if let Some(connection) = self.active_connections.remove(id) {
                            self.inactive_connections.insert(id.to_owned(), InactiveConnection::new(connection.state));
                        }
                    }
                    TrySendError::Closed(_m) => {
                        error!("Closed connection. Forward failed");
                        self.active_connections.remove(id);
                    }
                }
            }
        }
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
