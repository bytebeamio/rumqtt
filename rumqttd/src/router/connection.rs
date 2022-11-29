use crate::protocol::LastWill;
use crate::Filter;
use std::collections::HashSet;

use super::ConnectionEvents;

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Debug)]
pub struct Connection {
    pub client_id: String,
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
    /// Connection events
    pub events: ConnectionEvents,
}

impl Connection {
    /// Create connection state to hold identifying information of connecting device
    pub fn new(
        tenant_id: Option<String>,
        client_id: String,
        clean: bool,
        last_will: Option<LastWill>,
        dynamic_filters: bool,
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
            tenant_prefix,
            dynamic_filters,
            clean,
            subscriptions: HashSet::default(),
            last_will,
            events: ConnectionEvents::default(),
        }
    }
}
