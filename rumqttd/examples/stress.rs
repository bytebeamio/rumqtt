use std::time::Duration;

use bytes::Bytes;
use rumqttd::{
    local::LinkRx,
    protocol::{Packet, Publish, QoS},
    Broker,
};

use tokio::{
    select, task,
    time::{self, Instant},
};
use tracing_subscriber::EnvFilter;

const CONNECTIONS: usize = 200;
const MAX_MSG_PER_PUB: usize = 5;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // RUST_LOG=rumqttd[{client_id=consumer}]=debug cargo run --example stress
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(EnvFilter::from_env("RUST_LOG"));
        // .with_env_filter("rumqttd=debug");

    builder.try_init().unwrap();

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config = format!("{manifest_dir}/demo.toml");
    let config = config::Config::builder()
        .add_source(config::File::with_name(&config))
        .build()
        .unwrap(); // Config::default() doesn't have working values

    let config = config.try_deserialize().unwrap();
    let broker = Broker::new(config);

    let (mut link_tx, mut link_rx) = broker
        .link("consumer")
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

        let topic: Bytes = topic.into();
        let payload: Bytes = payload.into();
        task::spawn(async move {
            for _ in 0..MAX_MSG_PER_PUB {
                time::sleep(Duration::from_secs(1)).await;

                let publish = Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic: topic.clone(),
                    pkid: 0,
                    payload: payload.clone(),
                };

                link_tx.send(Packet::Publish(publish, None)).await.unwrap();
            }
        });
    }

    consumer(link_rx).await
}

async fn consumer(mut link_rx: LinkRx) {
    let mut count = 0;
    let mut interval = time::interval(Duration::from_secs(1));
    let instant = Instant::now();
    loop {
        select! {
            _ = interval.tick() => {
                println!("TOTAL COUNT: {count:?}; TIME: {:?}", instant.elapsed());
            }
            notification = link_rx.next() => {
                let notification = notification.unwrap();
                if notification.is_some() {
                    count += 1;
                }
            }
        }
    }
}
