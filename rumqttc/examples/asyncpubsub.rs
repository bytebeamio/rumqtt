use async_channel::Sender;
use tokio::{task, time};

use rumqttc::{self, EventLoop, MqttOptions, Publish, QoS, Request, Subscribe};
use std::error::Error;
use std::time::Duration;

#[tokio::main(core_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5);

    let mut eventloop = EventLoop::new(mqttoptions, 10).await;
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        requests(requests_tx).await;
        time::delay_for(Duration::from_secs(3)).await;
    });

    loop {
        let (incoming, outgoing) = eventloop.poll().await?;
        println!("Incoming = {:?}, Outgoing = {:?}", incoming, outgoing);
    }
}

async fn requests(requests_tx: Sender<Request>) {
    let subscription = Subscribe::new("hello/world", QoS::AtMostOnce);
    let _ = requests_tx.send(Request::Subscribe(subscription)).await;

    for i in 0..10 {
        let publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![i; i as usize]);
        requests_tx.send(Request::Publish(publish)).await.unwrap();
        time::delay_for(Duration::from_secs(1)).await;
    }

    time::delay_for(Duration::from_secs(120)).await;
}
