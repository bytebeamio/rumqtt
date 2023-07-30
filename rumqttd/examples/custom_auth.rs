use pwhash::bcrypt;
use rumqttd::{protocol::Connect, AuthStatus, Authenticator, Broker, Config, ServerSettings};
use serde::Deserialize;

use std::{collections::HashMap, str::FromStr, sync::Arc, thread};

#[derive(Deserialize, Debug)]
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
        return &publish.topic == self.topic.as_bytes();
    }

    fn authorize_notify(&self, publish: &rumqttd::protocol::Publish) -> bool {
        return &publish.topic == self.topic.as_bytes();
    }
}

#[derive(Debug, Deserialize)]
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
        let login = match login {
            Some(l) => l,
            None => return None,
        };
        let user = match self.users.get(&login.username) {
            Some(u) => u,
            None => return None,
        };
        if !bcrypt::verify(login.password, &user.pwhash) {
            return None;
        }
        return Some(Box::new(MyConnAuthStatus {
            topic: user.topic.clone(),
        }));
    }
}

fn main() {
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false);

    builder
        .try_init()
        .expect("initialized subscriber succesfully");

    let mut users_config = HashMap::new();
    users_config.insert(
        "myuser1".to_string(),
        UserConfig {
            pwhash: bcrypt::hash("$2y$05$bvIG6Nmid91Mu9RcmmWZfO5HJIMCT8riNW0hEp8f6/FuA2/mHZFpe")
                .unwrap(),
            topic: "users/user1".to_string(),
        },
    );
    let mut v4_config = HashMap::new();
    v4_config.insert(
        "main".to_string(),
        ServerSettings {
            name: "main_again".into(),
            listen: std::net::SocketAddr::from_str("0.0.0.0:1443").unwrap(),
            connections: rumqttd::ConnectionSettings {
                dyn_auth: Some(Arc::new(MyCustomAuth {
                    users: users_config,
                })),
                auth: None,
                connection_timeout_ms: 60000,
                max_payload_size: 20480,
                max_inflight_count: 500,
                dynamic_filters: true,
            },
            next_connection_delay_ms: 1,
            tls: None,
        },
    );
    let mut broker = Broker::new(Config {
        v4: v4_config,
        ..Default::default()
    });

    let (mut link_tx, mut link_rx) = broker.link("singlenode").unwrap();
    thread::spawn(move || {
        broker.start().unwrap();
    });

    link_tx.subscribe("#").unwrap();

    loop {
        match link_rx.recv().unwrap() {
            Some(v) => {
                println!("{v:?}");
            }
            None => {}
        }
    }
}
