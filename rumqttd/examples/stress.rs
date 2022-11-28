use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use rumqttd::{
    local::LinkRx,
    protocol::{Packet, Publish},
    Broker, Notification,
};

use tokio::{
    select,
    sync::Barrier,
    task,
    time::{self, Instant},
};
use tracing_subscriber::EnvFilter;

const PUBLISHERS: usize = 10000;
const MAX_MSG_PER_PUB: usize = 100;
const SLEEP_TIME_MS_BETWEEN_PUB: u64 = 100;
const CONSUMERS: usize = 2;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // RUST_LOG=rumqttd[{client_id=consumer}]=debug cargo run --example stress
    tracing_subscriber::fmt()
        // .with_env_filter("rumqttd=debug");
        .with_env_filter(EnvFilter::from_env("RUST_LOG"))
        .pretty()
        .init();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config = format!("{manifest_dir}/demo.toml");
    let config = config::Config::builder()
        .add_source(config::File::with_name(&config))
        .build()
        .unwrap(); // Config::default() doesn't have working values

    let config = config.try_deserialize().unwrap();
    let broker = Broker::new(config);

    for i in 0..CONSUMERS {
        let client_id = format!("consumer_{i}");
        let (mut link_tx, mut link_rx) = broker.link(&client_id).unwrap();

        link_tx.subscribe("hello/+/world").unwrap();
        link_rx.recv().unwrap();

        task::spawn(async move { consumer(&client_id, link_rx).await });
    }

    let barrier = Arc::new(Barrier::new(PUBLISHERS + 1));
    for i in 0..PUBLISHERS {
        let c = barrier.clone();
        let client_id = format!("publisher_{i}");
        let topic = format!("hello/{}/world", client_id);
        let payload = vec![0u8; 1_000]; // 0u8 is one byte, so total ~1KB
        let (mut link_tx, _link_rx) = broker.link(&client_id).unwrap();

        let topic: Bytes = topic.into();
        let payload: Bytes = payload.into();
        task::spawn(async move {
            for _ in 0..MAX_MSG_PER_PUB {
                time::sleep(Duration::from_millis(SLEEP_TIME_MS_BETWEEN_PUB)).await;
                let publish = Publish::new(topic.clone(), payload.clone(), false);
                link_tx.send(Packet::Publish(publish, None)).await.unwrap();
            }

            c.wait().await;
        });
    }

    barrier.wait().await;
    time::sleep(Duration::from_secs(5)).await;
}

async fn consumer(client_id: &str, mut link_rx: LinkRx) {
    let mut count = 0;
    let mut interval = time::interval(Duration::from_millis(500));
    let instant = Instant::now();
    loop {
        select! {
            _ = interval.tick() => {
                println!("{client_id:?}: total count: {count:<10}; time: {:?}", instant.elapsed());
            }
            notification = link_rx.next() => {
                let notification = match notification.unwrap() {
                    Some(v) => v,
                    None => continue
                };

                match notification {
                    Notification::Forward(_) => count += 1,
                    Notification::Unschedule => link_rx.wake().await.unwrap(),
                    _ => unreachable!()
                }
            }
        }
    }
}
