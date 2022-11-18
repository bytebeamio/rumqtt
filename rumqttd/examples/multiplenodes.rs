use rumqttd::Broker;

fn main() {
    // let router = Router::new(); // Router is not publically exposed!
    tracing_subscriber::fmt::init();
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config = config::Config::builder()
        .add_source(config::File::with_name(&format!(
            "{manifest_dir}/demo.toml"
        )))
        .build()
        .unwrap(); // Config::default() doesn't have working values

    let config = config.try_deserialize().unwrap();
    let broker = Broker::new(config);

    const CONNECTIONS: usize = 10;

    let (mut link_tx, mut link_rx) = broker
        .link("the_subscriber")
        .expect("New link should be made");
    link_tx
        .subscribe("hello/+/world")
        .expect("link should subscribe");

    link_rx.recv().expect("Should recieve Ack");

    for i in 0..CONNECTIONS {
        let client_id = format!("client_{i}");
        let topic = format!("hello/{}/world", client_id);
        let payload = vec![0u8; 1_000]; // 0u8 is one byte, so total ~1KB
        let (mut link_tx, _link_rx) = broker.link(&client_id).expect("New link should be made");
        link_tx.publish(topic, payload).unwrap();
    }

    let mut count = 0;
    loop {
        let notification = link_rx.recv().unwrap();
        if notification.is_some() {
            count += 1;
        }
        // do we need to print what data we got?
        println!("RECV {notification:?}");
        dbg!(count);
    }
}
