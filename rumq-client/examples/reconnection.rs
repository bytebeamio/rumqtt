use futures_util::stream::StreamExt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

use rumq_client::{self, QoS, MqttOptions, Request, MqttEventLoop, eventloop};
use std::time::Duration;

#[tokio::main(basic_scheduler)]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "test.mosquitto.org", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));

    let (requests_tx, requests_rx) = channel(10);
    let mut eventloop = eventloop(mqttoptions, requests_rx);

    // start sending requests
    task::spawn(async move {
        requests(requests_tx).await;
    });


    stream_it(&mut eventloop).await;
}

async fn stream_it(eventloop: &mut MqttEventLoop) {
    loop {
        let mut stream = eventloop.stream();
        while let Some(item) = stream.next().await {
            println!("Received = {:?}", item);
        }

        time::delay_for(Duration::from_secs(1)).await;
    }
}

async fn requests(mut requests_tx: Sender<Request>) {
    let topic = "hello/world".to_owned();

    for i in 0..10 {
        let payload = vec![1, 2, 3, i];
        let publish = rumq_client::publish(&topic, QoS::AtLeastOnce, payload);
        let publish = Request::Publish(publish);

        requests_tx.send(publish).await.unwrap();
        time::delay_for(Duration::from_secs(1)).await; 
    }
        
    time::delay_for(Duration::from_secs(300)).await;
}
