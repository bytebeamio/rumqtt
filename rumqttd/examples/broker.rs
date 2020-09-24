use librumqttd::Broker;
use serde::{Deserialize, Serialize};
use std::thread;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct Config {
    broker: librumqttd::Config,
}

fn main() {
    pretty_env_logger::init();
    let config: Config = confy::load_path("config/rumqttd.conf").unwrap();
    let mut broker = Broker::new(config.broker);

    let mut tx = broker.link("localclient", 10).unwrap();
    thread::spawn(move || {
        let mut rx = tx.connect().await.unwrap();
        tx.subscribe("#").await.unwrap();
        loop {
            if let Some(message) = rx.recv().await.unwrap() {
                println!(
                    "Incoming. Topic = {}, Payload = {:?}",
                    message.topic, message.payload
                );
            }
        }
    });

    broker.start().unwrap();
}
