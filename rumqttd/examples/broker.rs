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

    let mut tx = broker.link("localclient").unwrap();
    thread::spawn(move || {
        broker.start().unwrap();
    });

    // connect to get a receiver
    // TODO: Connect with a function which return tx and rx to prevent
    // doing publishes before connecting
    // NOTE: Connection buffer should be atleast total number of possible
    // topics + 3 (request types). If inflight is full with more topics
    // in tracker, it's possible that router never responnds current
    // inflight requests. But other pending requests should still be able
    // to progress
    let mut rx = tx.connect(200).unwrap();
    tx.subscribe("#").unwrap();

    // subscribe and publish in a separate thread
    thread::spawn(move || {
        for _ in 0..10 {
            for i in 0..200 {
                let topic = format!("hello/{}/world", i);
                tx.publish(topic, false, vec![0; 1024]).unwrap();
            }
        }
    });

    let mut count = 0;
    loop {
        if let Some(message) = rx.recv().unwrap() {
            // println!("T = {}, P = {:?}", message.topic, message.payload.len());
            count += message.payload.len();
            println!("{}", count);
        }
    }
}
