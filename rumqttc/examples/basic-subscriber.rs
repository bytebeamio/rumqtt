use tokio::{task, time};

use rumqttc::{self, AsyncClient, Event, MqttOptions, Packet, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("basic-subscriber", "localhost", 1884);
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
            Event::Incoming(Packet::Publish(packet)) => {
                i += 1;
                println!("[{i}] Incoming publish: {:?}", packet);
            }
            Event::Incoming(e) => {
                println!(" Incoming event: {:?}", e);
            }
            _ => continue,
        }
    }
}

async fn requests(client: AsyncClient) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();
}
