use slab::Slab;

use crate::protocol::LastWillProperties;
use crate::{
    acl::{AclAction, AclHandler, ClientIdentity},
    Filter,
};
use crate::{protocol::LastWill, Topic};
use std::collections::{HashMap, HashSet};

use super::ConnectionEvents;

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
#[derive(Debug)]
pub struct Connection {
    pub client_id: String,
    /// Authenticated identity used for ACL decisions.
    pub identity: ClientIdentity,
    /// Optional topic authorization policy for this connection.
    pub(crate) acl_handler: Option<AclHandler>,
    /// Id of client's organisation/tenant and the prefix associated with tenant's MQTT topic
    pub tenant_prefix: Option<String>,
    /// Dynamically create subscription filters incase they didn't exist during a publish
    pub dynamic_filters: bool,
    /// Clean session
    pub clean: bool,
    /// Subscriptions
    pub subscriptions: HashSet<Filter>,
    /// Last will of this connection
    pub last_will: Option<LastWill>,
    /// Properties of Last will
    pub last_will_properties: Option<LastWillProperties>,
    /// Connection events
    pub events: ConnectionEvents,
    /// Topic aliases set by clients
    pub(crate) topic_aliases: HashMap<u16, Topic>,
    /// Topic aliases used by broker
    pub(crate) broker_topic_aliases: Option<BrokerAliases>,
    /// subscription IDs for a connection
    pub(crate) subscription_ids: HashMap<Filter, usize>,
}

impl Connection {
    /// Create connection state to hold identifying information of connecting device
    pub fn new(
        tenant_id: Option<String>,
        client_id: String,
        clean: bool,
        dynamic_filters: bool,
        identity: ClientIdentity,
    ) -> Connection {
        // Change client id to -> tenant_id.client_id and derive topic path prefix
        // to validate topics
        let (client_id, tenant_prefix) = match tenant_id {
            Some(tenant_id) => {
                let tenant_prefix = Some("/tenants/".to_owned() + &tenant_id + "/");
                let client_id = tenant_id + "." + &client_id;
                (client_id, tenant_prefix)
            }
            None => (client_id, None),
        };

        Connection {
            client_id,
            identity,
            acl_handler: None,
            tenant_prefix,
            dynamic_filters,
            clean,
            subscriptions: HashSet::default(),
            last_will: None,
            last_will_properties: None,
            events: ConnectionEvents::default(),
            topic_aliases: HashMap::new(),
            broker_topic_aliases: None,
            subscription_ids: HashMap::new(),
        }
    }

    pub fn topic_alias_max(&mut self, max: u16) -> &mut Connection {
        // if topic_alias_max is 0, that means client doesn't want to use / support topic alias
        if max > 0 {
            self.broker_topic_aliases = Some(BrokerAliases::new(max));
        }
        self
    }

    pub fn last_will(
        &mut self,
        will: Option<LastWill>,
        props: Option<LastWillProperties>,
    ) -> &mut Connection {
        self.last_will = will;
        self.last_will_properties = props;
        self
    }

    pub fn acl_handler(&mut self, handler: Option<AclHandler>) -> &mut Self {
        self.acl_handler = handler;
        self
    }

    pub fn allows(&self, action: AclAction, topic: &str) -> bool {
        match &self.acl_handler {
            Some(handler) => handler.allows(&self.identity, action, topic),
            None => true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BrokerAliases {
    pub(crate) broker_topic_aliases: HashMap<Filter, u16>,
    pub(crate) used_aliases: Slab<()>,
    pub(crate) topic_alias_max: u16,
}

impl BrokerAliases {
    fn new(topic_alias_max: u16) -> BrokerAliases {
        let mut used_aliases = Slab::new();
        // occupy 0th index as 0 is invalid topic alias
        assert_eq!(0, used_aliases.insert(()));

        let broker_topic_aliases = HashMap::new();

        BrokerAliases {
            broker_topic_aliases,
            used_aliases,
            topic_alias_max,
        }
    }

    // unset / remove the alias for topic
    pub fn remove_alias(&mut self, topic: &str) {
        if let Some(alias) = self.broker_topic_aliases.remove(topic) {
            self.used_aliases.remove(alias as usize);
        }
    }

    // Get alias used for the topic, if it exists
    pub fn get_alias(&self, topic: &str) -> Option<u16> {
        self.broker_topic_aliases.get(topic).copied()
    }

    // Set new alias for a topic and return the alias
    // returns None if can't set new alias
    pub fn set_new_alias(&mut self, topic: &str) -> Option<u16> {
        let alias_to_use = self.used_aliases.insert(());

        // NOTE: maybe we can use self.used_aliases.len()
        // to check for availability of alias
        if alias_to_use > self.topic_alias_max as usize {
            self.used_aliases.remove(alias_to_use);
            return None;
        }

        let alias_to_use = alias_to_use as u16;
        self.broker_topic_aliases
            .insert(topic.to_owned(), alias_to_use);
        Some(alias_to_use)
    }
}

#[cfg(test)]
mod tests {
    use super::Connection;
    use crate::{AclAction, AclHandler, ClientIdentity};

    #[test]
    fn connection_keeps_acl_identity() {
        let identity =
            ClientIdentity::authenticated("client-1", "user-1", Some("tenant-1".to_owned()));
        let connection =
            Connection::new(None, "client-1".to_owned(), true, false, identity.clone());

        assert_eq!(connection.identity, identity);
    }

    #[test]
    fn connection_applies_publish_acl() {
        let identity = ClientIdentity::unauthenticated("client-1", None);
        let mut connection = Connection::new(None, "client-1".to_owned(), true, false, identity);
        connection.acl_handler(Some(AclHandler::new(|_, action, topic| {
            action == AclAction::Publish && topic == "allowed/topic"
        })));

        assert!(connection.allows(AclAction::Publish, "allowed/topic"));
        assert!(!connection.allows(AclAction::Publish, "denied/topic"));
    }
}
