use std::thread;

use rumqttd::Broker;

fn main() {
    // let router = Router::new(); // Router is not publically exposed!
    let config = config::Config::builder()
        .add_source(config::File::with_name("demo.toml"))
        .build()
        .unwrap(); // Config::default() doesn't have working values

    let config = config.try_deserialize().unwrap();
    println!("{config:?}");
    let mut broker = Broker::new(config);

    const CONNECTIONS: usize = 10;
    const MESSAGES_PER_CONNECTION: usize = 2;

    let mut rxs = Vec::with_capacity(CONNECTIONS);
    for i in 0..CONNECTIONS {
        let client_id = format!("client_{i}");
        let (mut link_tx, link_rx) = broker.link(&client_id).expect("New link should be made");
        rxs.push(link_rx);
        thread::spawn(move || {
            for _ in 0..MESSAGES_PER_CONNECTION {
                link_tx.publish("hello/world", vec![1, 2, 3]).unwrap();
            }
        });
    }

    thread::spawn(move || {
        broker.start().unwrap();
    });
    for mut rx in rxs {
        let not = rx.recv().unwrap();
        println!("RECV {not:?}");
    }
}
