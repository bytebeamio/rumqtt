use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub struct AclHandler(Arc<dyn Fn(&ClientIdentity, AclAction, &str) -> bool + Send + Sync>);

impl AclHandler {
    pub fn new<F>(acl_fn: F) -> Self
    where
        F: Fn(&ClientIdentity, AclAction, &str) -> bool + Send + Sync + 'static,
    {
        Self(Arc::new(acl_fn))
    }

    pub fn allows(&self, identity: &ClientIdentity, action: AclAction, topic: &str) -> bool {
        (self.0)(identity, action, topic)
    }
}

impl fmt::Debug for AclHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AclHandler(..)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientIdentity {
    pub client_id: String,
    /// Username accepted by the configured authentication mechanism.
    pub username: Option<String>,
    pub tenant_id: Option<String>,
}

impl ClientIdentity {
    pub fn unauthenticated(client_id: impl Into<String>, tenant_id: Option<String>) -> Self {
        Self {
            client_id: client_id.into(),
            username: None,
            tenant_id,
        }
    }
    pub fn authenticated(
        client_id: impl Into<String>,
        username: impl Into<String>,
        tenant_id: Option<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            username: Some(username.into()),
            tenant_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclAction {
    Publish,
    Subscribe,
    Unsubscribe,
    Will,
}

#[cfg(test)]
mod tests {
    use super::{AclAction, ClientIdentity};

    #[test]
    fn authenticated_identity_preserves_verified_username() {
        let identity =
            ClientIdentity::authenticated("client-1", "user-1", Some("tenant-1".to_owned()));

        assert_eq!(identity.client_id, "client-1");
        assert_eq!(identity.username.as_deref(), Some("user-1"));
        assert_eq!(identity.tenant_id.as_deref(), Some("tenant-1"));
    }

    #[test]
    fn unauthenticated_identity_does_not_claim_username() {
        let identity = ClientIdentity::unauthenticated("client-1", None);

        assert_eq!(identity.username, None);
    }

    #[test]
    fn acl_actions_distinguish_topic_operations() {
        assert_ne!(AclAction::Publish, AclAction::Subscribe);
        assert_ne!(AclAction::Subscribe, AclAction::Unsubscribe);
        assert_ne!(AclAction::Unsubscribe, AclAction::Will);
    }
}
