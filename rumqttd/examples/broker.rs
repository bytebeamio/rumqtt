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
        broker.start().unwrap();
    });

    // connect to get a receiver
    // TODO: Connect with a function which return tx and rx to prevent
    // doing publishes before connecting
    let mut rx = tx.connect().unwrap();
    tx.subscribe("#").unwrap();

    // subscribe and publish in a separate thread
    thread::spawn(move || {
        for _i in 0..10 {
            tx.publish("hello/0/world", false, vec![0; 1024]).unwrap();
        }
    });

    loop {
        if let Some(message) = rx.recv().unwrap() {
            println!("T = {}, P = {:?}", message.topic, message.payload.len());
        }
    }
}
