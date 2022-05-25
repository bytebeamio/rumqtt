#![allow(dead_code, unused_imports)]
use tokio::{task, time};

use rumqttc::v5::{connect, AsyncClient, MqttOptions, Notifier, QoS, Packet};
use std::error::Error;
use std::time::Duration;

async fn create_conn() -> (AsyncClient, Notifier) {
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions
        .set_keep_alive(Duration::from_secs(5))
        .set_manual_acks(true)
        .set_clean_session(false);

    connect(mqttoptions, 10).await.unwrap()
}

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    // todo!("fix this example with new way of spawning clients")
    pretty_env_logger::init();

    // create mqtt connection with clean_session = false and manual_acks = true
    let (client, mut notifier) = create_conn().await;

    // subscribe example topic
    client
        .subscribe("hello/world", QoS::AtLeastOnce)
        .await
        .unwrap();

    task::spawn(async move {
        // send some messages to example topic and disconnect
        requests(&client).await;
        client.disconnect().await.unwrap()
    });

    // get subscribed messages without acking
    for event in notifier.iter() {
        println!("{:?}", event);
    }

    // create new broker connection
    let (client, mut notifier) = create_conn().await;

    for event in notifier.iter() {
        // previously published messages should be republished after reconnection.
        println!("{:?}", event);

        match event {
            Packet::Publish(publish) => {
                // this time we will ack incoming publishes.
                // Its important not to block notifier as this can cause deadlock.
                let c = client.clone();
                tokio::spawn(async move {
                    c.ack(&publish).await.unwrap();
                });
            }
            _ => {}
        }
    }

    Ok(())
}

async fn requests(client: &AsyncClient) {
    for i in 1..=10 {
        client
            .publish("hello/world", QoS::AtLeastOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }
}
