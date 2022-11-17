use std::{thread, time::Duration};

use rumqttd::Broker;

fn main() {
    // let router = Router::new(); // Router is not publically exposed!
    tracing_subscriber::fmt::init();
    let config = config::Config::builder()
        .add_source(config::File::with_name("demo.toml"))
        .build()
        .unwrap(); // Config::default() doesn't have working values

    let config = config.try_deserialize().unwrap();
    println!("{config:?}");
    let broker = Broker::new(config);

    const CONNECTIONS: usize = 10;
    const MESSAGES_PER_CONNECTION: usize = 2;

    let mut rxs = Vec::with_capacity(CONNECTIONS);
    for i in 0..CONNECTIONS {
        let client_id = format!("client_{i}");
        let (mut link_tx, link_rx) = broker.link(&client_id).expect("New link should be made");
        if i.is_power_of_two() { // 0, 1, 2, 4, 8 .. just to make some subscriber clients
            link_tx.subscribe("hello/world").unwrap();
            rxs.push(link_rx);
        } else {
            thread::spawn(move || {
                thread::sleep(Duration::from_secs(1));
                for _ in 0..MESSAGES_PER_CONNECTION {
                    link_tx.publish("hello/world", vec![1, 2, 3]).unwrap();
                }
            });
        }
    }

    loop {
        for rx in &mut rxs {
            let not = rx.recv().unwrap();
            println!("RECV {not:?}");
        }
    }
}
