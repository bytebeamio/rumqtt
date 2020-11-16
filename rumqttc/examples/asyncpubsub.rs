use tokio::{task, time};

use rumqttc::{self, AsyncClient, Event, Incoming, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
        time::sleep(Duration::from_secs(3)).await;
    });

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                println!("Topic: {}, Payload: {:?}", p.topic, p.payload)
            }
            Ok(Event::Incoming(i)) => {
                println!("Incoming = {:?}", i);
            }
            Ok(Event::Outgoing(o)) => println!("Outgoing = {:?}", o),
            Err(e) => {
                println!("Error = {:?}", e);
                continue;
            }
        }
    }
}

async fn requests(client: AsyncClient) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .publish("hello/world", QoS::ExactlyOnce, false, vec![i; 1000 * 1024])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}
