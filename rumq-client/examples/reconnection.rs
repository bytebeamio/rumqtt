use futures_util::stream::StreamExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use tokio::time;

use rumq_client::{self, eventloop, MqttEventLoop, MqttOptions, Publish, QoS, Request};
use std::time::Duration;

#[tokio::main(basic_scheduler)]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));

    let (requests_tx, requests_rx) = channel(10);
    let mut eventloop = eventloop(mqttoptions, requests_rx);

    // start sending requests
    task::spawn(async move {
        requests(requests_tx).await;
        time::delay_for(Duration::from_secs(30)).await;
    });

    loop {
        stream_it(&mut eventloop).await;
        time::delay_for(Duration::from_secs(5)).await;
    }
}

async fn stream_it(eventloop: &mut MqttEventLoop<Receiver<Request>>) {
    let mut stream = eventloop.connect().await.unwrap();

    while let Some(item) = stream.next().await {
        println!("Received = {:?}", item);
    }

    println!("Stream done");
}

async fn requests(mut requests_tx: Sender<Request>) {
    let topic = "hello/world".to_owned();

    for i in 0..10 {
        let payload = vec![1, 2, 3, i];
        let publish = Publish::new(&topic, QoS::AtLeastOnce, payload);
        let publish = Request::Publish(publish);

        requests_tx.send(publish).await.unwrap();
        time::delay_for(Duration::from_secs(1)).await;
    }
}
