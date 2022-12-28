use tokio::{task, time};

use rumqttc::{self, AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("basic-publisher", "localhost", 1884);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
        time::sleep(Duration::from_secs(3)).await;
    });

    let mut i = 0;
    loop {
        let event = eventloop.poll().await;
        match event.unwrap() {
            Event::Incoming(Packet::PubAck(packet)) => {
                println!("[{i}] Incoming puback: {:?}", packet);
            }
            Event::Outgoing(Outgoing::Publish(packet)) => {
                i += 1;
                println!("[{i}] Outgoing publish: {:?}", packet);
            }
            _ => continue,
        }
    }
}

async fn requests(client: AsyncClient) {
    for _ in 1..=10 {
        client
            .publish("hello/world", QoS::AtLeastOnce, false, vec![1; 1024])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(3)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}
