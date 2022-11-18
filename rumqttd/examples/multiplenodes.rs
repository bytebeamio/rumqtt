use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

use rumqttd::{Broker, LinkRx};
use tokio::time::{self, Instant};

#[tokio::main]
async fn main() {
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
    const MAX_MSG_PER_PUB: usize = 5;

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
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            for _ in 0..MAX_MSG_PER_PUB {
                interval.tick().await;
                link_tx.publish(topic.clone(), payload.clone()).unwrap();
            }
        });
    }

    let count = Arc::new(AtomicU32::new(0));
    let instant = Instant::now();

    tokio::spawn(keep_recv(link_rx, count.clone()));

    let eta = MAX_MSG_PER_PUB + 2; // 2 sec as buffer time / delay

    let mut interval = time::interval(Duration::from_secs(1));
    for _ in 0..eta {
        interval.tick().await;
        println!("TOTAL COUNT: {count:?}; TIME: {:?}", instant.elapsed());
    }
}

async fn keep_recv(mut link_rx: LinkRx, count: Arc<AtomicU32>) {
    loop {
        let notification = link_rx.recv().unwrap();
        if notification.is_some() {
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
