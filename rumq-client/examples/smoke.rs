use futures_util::stream::StreamExt;
use std::thread;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

use rumq_client::{self, MqttOptions, ReconnectOptions, Request, MqttEventLoop, eventloop};
use std::time::Duration;

#[tokio::main(basic_scheduler)]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (requests_tx, requests_rx) = channel(10);
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));
    let mut eventloop = eventloop(mqttoptions, requests_rx).await.unwrap();

    thread::spawn(move || {
        requests(requests_tx);
        thread::sleep(Duration::from_secs(3));
    });


    stream_it(&mut eventloop).await;
    println!("State = {:?}", eventloop.state);
}


async fn stream_it(eventloop: &mut MqttEventLoop) {
    let mut stream = eventloop.stream(ReconnectOptions::Always(Duration::from_secs(5)));

    while let Some(item) = stream.next().await {
        println!("{:?}", item);
    }
}


#[tokio::main(basic_scheduler)]
async fn requests(mut requests_tx: Sender<Request>) {
    task::spawn(async move {
        for i in 0..10 {
            requests_tx.send(publish_request(i)).await.unwrap();
            time::delay_for(Duration::from_secs(1)).await; 
        }

        time::delay_for(Duration::from_secs(30)).await; 
    }).await.unwrap();
}

fn publish_request(i: u8) -> Request {
    let topic = "hello/world".to_owned();
    let payload = vec![1, 2, 3, i];

    let publish = rumq_client::publish(topic, payload);
    Request::Publish(publish)
}
