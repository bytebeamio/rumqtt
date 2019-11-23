use futures_util::stream::StreamExt;
use std::thread;
use rumq_core::*;
use std::sync::Arc;
use async_std::sync::channel;
use async_std::task;

use rumq_client::{self, MqttOptions, Request, connect};
use std::time::Duration;

#[async_std::main]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (requests_tx, requests_rx) = channel(10);
    let mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    let mqttoptions = mqttoptions.set_keep_alive(10);

    let timeout = Duration::from_secs(5);
    let mut eventloop = connect(mqttoptions, timeout).await.unwrap();
    let mut stream = eventloop.build(requests_rx).await.unwrap();

    thread::spawn(move || {
        task::block_on( async {
            for i in 0..10 {
                requests_tx.send(publish(i)).await;
                task::sleep(Duration::from_secs(1)).await;
            }
        });

        thread::sleep(Duration::from_secs(100));
    });

    while let Some(item) = stream.next().await {
        println!("{:?}", item);
    }
}

fn publish(i: u8) -> Request {
    let publish = Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic_name: "hello/world".to_owned(),
        pkid: None,
        payload: Arc::new(vec![1, 2, 3, i])
    };

    Request::Publish(publish)
}