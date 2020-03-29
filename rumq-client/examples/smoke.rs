use futures_util::stream::StreamExt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

use rumq_client::{self, QoS, MqttOptions, Publish, Request, MqttEventLoop, eventloop};
use std::time::Duration;

#[tokio::main(basic_scheduler)]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (requests_tx, requests_rx) = channel(10);
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(30).set_throttle(Duration::from_secs(1));
    let mut eventloop = eventloop(mqttoptions, requests_rx);

    task::spawn(async move {
        requests(requests_tx).await;
    });

    stream_it(&mut eventloop).await;
}


async fn stream_it(eventloop: &mut MqttEventLoop) {
    let mut stream = eventloop.connect().await.unwrap();

    while let Some(item) = stream.next().await {
        println!("Received = {:?}", item);
    }

    println!("Stream done");
}

async fn requests(mut requests_tx: Sender<Request>) {
    for i in 0..10 {
        requests_tx.send(publish_request(i)).await.unwrap();
        time::delay_for(Duration::from_secs(1)).await;
    }

    time::delay_for(Duration::from_secs(600)).await;
}

fn publish_request(i: u8) -> Request {
    let topic = "hello/world".to_owned();
    let payload = vec![1, 2, 3, i];

    let publish = Publish::new(&topic, QoS::AtLeastOnce, payload);
    Request::Publish(publish)
}
