use futures_util::stream::StreamExt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

use rumq_client::{self, MqttState, MqttOptions, Request, Notification, MqttEventLoop, eventloop};
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
        time::delay_for(Duration::from_secs(3)).await;
    });


    loop {
        let (state, options) = stream_it(&mut eventloop).await;
        println!("state = {:?}\noptions={:?}", state, options);
        time::delay_for(Duration::from_secs(5)).await;
    }
}

async fn stream_it(eventloop: &mut MqttEventLoop) -> (MqttState, MqttOptions) {
    while let Some(item) = eventloop.next().await {
        if let Notification::StreamEnd(err, options, state) = item {
            println!("Error = {:?}", err);
            return (state, options)
        }

        println!("Received = {:?}", item);
    }

    let options = MqttOptions::new("test", "localhost", 1883);
    let state = MqttState::new();
    (state, options)
}

async fn requests(mut requests_tx: Sender<Request>) {
    let topic = "hello/world".to_owned();

    for i in 0..100 {
        let payload = vec![1, 2, 3, i];
        let publish = rumq_client::publish(&topic, payload);
        let publish = Request::Publish(publish);

        requests_tx.send(publish).await.unwrap();
        time::delay_for(Duration::from_secs(1)).await; 
    }
}
