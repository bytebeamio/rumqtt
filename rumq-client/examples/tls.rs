use futures_util::stream::StreamExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task;
use tokio::time;

use rumq_client::{self, eventloop, MqttEventLoop, MqttOptions, Publish, QoS, Request, Subscribe};
use std::fs;
use std::time::Duration;

#[tokio::main(basic_scheduler)]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let ca = fs::read("certs/tlsfiles/ca-chain.cert.pem").unwrap();
    let _client_cert = fs::read("certs/tlsfiles/device-1.cert.pem").unwrap();
    let _client_key = fs::read("certs/tlsfiles/device-1.key.pem").unwrap();

    let (requests_tx, requests_rx) = channel(10);
    let mut mqttoptions = MqttOptions::new("device-1", "mqtt.bytebeam.io", 8883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));
    mqttoptions.set_ca(ca);
    // mqttoptions.set_client_auth(client_cert, client_key);

    let mut eventloop = eventloop(mqttoptions, requests_rx);
    task::spawn(async move {
        requests(requests_tx).await;
        time::delay_for(Duration::from_secs(3)).await;
    });

    stream_it(&mut eventloop).await;
    // println!("State = {:?}", eventloop.state);
}

async fn stream_it(eventloop: &mut MqttEventLoop<Receiver<Request>>) {
    let mut stream = eventloop.connect().await.unwrap();

    while let Some(item) = stream.next().await {
        println!("Received = {:?}", item);
    }

    println!("Stream done");
}

async fn requests(mut requests_tx: Sender<Request>) {
    let subscription = Subscribe::new("hello/world", QoS::AtLeastOnce);
    let _ = requests_tx.send(Request::Subscribe(subscription)).await;

    for i in 0..10 {
        requests_tx.send(publish_request(i)).await.unwrap();
        time::delay_for(Duration::from_secs(1)).await;
    }

    time::delay_for(Duration::from_secs(60)).await;
}

fn publish_request(i: u8) -> Request {
    let topic = "hello/world".to_owned();
    let payload = vec![1, 2, 3, i];

    let publish = Publish::new(&topic, QoS::AtLeastOnce, payload);
    Request::Publish(publish)
}
