use futures_util::stream::StreamExt;
use std::thread;
use rumq_core::*;
use std::sync::Arc;
use tokio::sync::mpsc;

use rumq_client::{self, MqttOptions, Request, connect};
use std::time::Duration;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let (mut requests_tx, requests_rx) = mpsc::channel(1);
    let mqttoptions = MqttOptions::new("test-1", "localhost", 5555);

    let mut eventloop = connect(mqttoptions).await.unwrap();
    let mut stream = eventloop.build(requests_rx).await.unwrap();

    thread::spawn(move || {
        for i in 0..255 {
            let publish = publish(i);
            tokio_executor::current_thread::block_on_all(requests_tx.send(publish)).unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });

    while let Some(item) = stream.next().await {
        println!("{:?}", item);
    }

    thread::sleep_ms(10000);
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