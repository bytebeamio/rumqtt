use std::{thread};

use rumqttd::{Broker, Config};

fn main() {
    pretty_env_logger::init();

    // As examples are compiled as seperate binary so this config is current path dependent. Run it
    // from root of this crate
    let config = config::Config::builder()
        .add_source(config::File::with_name("demo.toml"))
        .build()
        .unwrap();

    let config: Config = config.try_deserialize().unwrap();
    let mut broker = Broker::new(config);

    let subscriptions = broker.subscriptions().unwrap();
    thread::spawn(move || {
        loop {
            println!("{:#?}", subscriptions.recv().unwrap());
        }
    });

    let (mut link_tx, mut link_rx) = broker.link("consumer").unwrap();
    link_tx.subscribe("hello/+/world").unwrap();
    thread::spawn(move || {
        loop {
            let notification = match link_rx.recv().unwrap() {
                Some(v) => v,
                None => continue,
            };

            println!("{:?}", notification);
        }
    });

    broker.start().unwrap();
}
