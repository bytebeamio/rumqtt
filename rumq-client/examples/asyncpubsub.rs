use tokio::stream::StreamExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::{task, time};

use rumq_client::{self, EventLoop, MqttOptions, Publish, QoS, Request, Subscribe, ConnectionError};
use std::time::Duration;

#[tokio::main(basic_scheduler)]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (requests_tx, requests_rx) = channel(10);
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);

    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));
    let mut eventloop = EventLoop::new(mqttoptions, requests_rx).await;
    task::spawn(async move {
        requests(requests_tx).await;
        time::delay_for(Duration::from_secs(3)).await;
    });

    let o = stream_it(&mut eventloop).await;
    println!("Done with the stream. out = {:?}", o);
}

async fn stream_it(eventloop: &mut EventLoop<Receiver<Request>>) -> Result<(), ConnectionError>{
    eventloop.connect().await?;

    loop {
        let (incoming, outgoing) = eventloop.poll().await?;
        println!("Incoming = {:?}, Outgoing = {:?}", incoming, outgoing);
    }
}

async fn requests(mut requests_tx: Sender<Request>) {
    let subscription = Subscribe::new("hello/world", QoS::AtMostOnce);
    let _ = requests_tx.send(Request::Subscribe(subscription)).await;

    for i in 0..10 {
        requests_tx.send(publish_request(i)).await.unwrap();
        time::delay_for(Duration::from_secs(1)).await;
    }

    time::delay_for(Duration::from_secs(120)).await;
}

fn publish_request(i: u8) -> Request {
    let topic = "hello/world".to_owned();
    let payload = vec![1, 2, 3, i];

    let publish = Publish::new(&topic, QoS::AtLeastOnce, payload);
    Request::Publish(publish)
}
