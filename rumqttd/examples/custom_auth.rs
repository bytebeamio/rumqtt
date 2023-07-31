use pwhash::bcrypt;
use rumqttd::{protocol::Connect, AuthStatus, Authenticator, Broker};
use serde::Deserialize;

use std::collections::HashMap;

#[derive(Clone, Deserialize, Debug)]
struct UserConfig {
    /// Password hash, in format accepted by linux shadow (crypt)
    pwhash: String,
    /// Only allow access to a single topic
    topic: String,
}

#[derive(Debug)]
struct MyConnAuthStatus {
    topic: String,
}

impl AuthStatus for MyConnAuthStatus {
    fn authorize_publish(&self, publish: &rumqttd::protocol::Publish) -> bool {
        publish.topic == self.topic.as_bytes()
    }

    fn authorize_notify(&self, publish: &rumqttd::protocol::Publish) -> bool {
        publish.topic == self.topic.as_bytes()
    }
}

#[derive(Clone, Debug, Deserialize)]
struct MyCustomAuth {
    users: HashMap<String, UserConfig>,
}

impl Authenticator for MyCustomAuth {
    fn authenticate(
        &self,
        _connect: &Connect,
        login: Option<rumqttd::protocol::Login>,
        _remote_addr: std::net::SocketAddr,
    ) -> Option<Box<dyn rumqttd::AuthStatus>> {
        let Some(login) = login else {
            return None;
        };

        let Some(user) = self.users.get(&login.username) else {
            return None;
        };

        if !bcrypt::verify(login.password, &user.pwhash) {
            return None;
        }

        Some(Box::new(MyConnAuthStatus {
            topic: user.topic.clone(),
        }))
    }
}

fn main() {
    let config = format!(
        "{manifest_dir}/rumqttd.toml",
        manifest_dir = env!("CARGO_MANIFEST_DIR")
    );
    let config = config::Config::builder()
        .add_source(config::File::with_name(&config))
        .build()
        .unwrap();

    let mut rumqttd_config: rumqttd::Config = config.try_deserialize().unwrap();
    let mut users_config = HashMap::new();
    users_config.insert(
        "myuser1".to_string(),
        UserConfig {
            pwhash: bcrypt::hash("$2y$05$bvIG6Nmid91Mu9RcmmWZfO5HJIMCT8riNW0hEp8f6/FuA2/mHZFpe")
                .unwrap(),
            topic: "users/user1".to_string(),
        },
    );

    let custom_auth = MyCustomAuth {
        users: users_config,
    };

    // NOTE: we can use v4.get("v4-1") to get specific config
    rumqttd_config
        .v4
        .iter_mut()
        .for_each(|(_, f)| f.connections.add_auth(custom_auth.clone()));

    let mut broker = Broker::new(rumqttd_config);

    broker.start().unwrap();
}
