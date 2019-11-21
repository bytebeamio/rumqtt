use futures_util::stream::StreamExt;
use std::thread;
use rumq_core::*;
use std::sync::Arc;
use futures_channel::mpsc;

use rumq_client::{self, MqttOptions, Request, connect};
use std::time::Duration;
use futures_util::sink::Sink;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (mut requests_tx, requests_rx) = mpsc::channel(1);
    let mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    let mqttoptions = mqttoptions.set_keep_alive(10);

    let timeout = Duration::from_secs(10);
    let mut eventloop = connect(mqttoptions, timeout).await.unwrap();
    let mut stream = eventloop.build(requests_rx).await.unwrap();

    thread::spawn(move || {
        for i in 0..10 {
            let publish = publish(i);
            futures_executor::block_on(requests_tx.send(publish)).unwrap();
            thread::sleep(Duration::from_secs(1));
        }
        thread::sleep(Duration::from_secs(300));
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