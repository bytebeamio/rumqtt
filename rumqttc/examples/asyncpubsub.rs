use tokio::{task, time};

use rumqttc::{self, AsyncClient, Event, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(core_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
        time::delay_for(Duration::from_secs(3)).await;
    });

    loop {
        match eventloop.poll().await? {
            Event::Incoming(i) => println!("Incoming = {:?}", i),
            Event::Outgoing(o) => println!("Outgoing = {:?}", o),
        }
    }
}

async fn requests(mut client: AsyncClient) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .publish("hello/world", QoS::AtLeastOnce, false, vec![i; i as usize])
            .await
            .unwrap();
        time::delay_for(Duration::from_secs(1)).await;
    }

    time::delay_for(Duration::from_secs(120)).await;
}
