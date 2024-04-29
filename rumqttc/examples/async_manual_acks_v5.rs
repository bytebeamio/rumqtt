use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use tokio::{task, time};

use rumqttc::v5::{AsyncClient, Event, EventLoop, ManualAckReason, MqttOptions};
use std::error::Error;
use std::time::Duration;

fn create_conn() -> (AsyncClient, EventLoop) {
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions
        .set_keep_alive(Duration::from_secs(5))
        .set_manual_acks(true)
        .set_clean_start(false)
        .set_session_expiry_interval(u32::MAX.into());

    AsyncClient::new(mqttoptions, 10)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    // todo!("fix this example with new way of spawning clients")
    pretty_env_logger::init();

    println!("");
    println!(">>>>>>>>>>> Create broker connection, do not ack broker publishes!!!");

    // create mqtt connection with clean_session = false and manual_acks = true
    let (client, mut eventloop) = create_conn();

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
    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(v) => {
                println!("Event = {v:?}");
            }
            Err(e) => {
                println!("Error = {e:?}");
                break;
            }
        }
    }

    println!("");
    println!(">>>>>>>>>>> Create new broker connection to get unack packets again!!!");

    // create new broker connection
    let (client, mut eventloop) = create_conn();

    while let Ok(event) = eventloop.poll().await {
        println!("{event:?}");

        if let Event::Incoming(packet) = event {
            let publish = match packet {
                Packet::Publish(publish) => publish,
                _ => continue,
            };
            // this time we will ack incoming publishes.
            // Its important not to block notifier as this can cause deadlock.
            let c = client.clone();
            tokio::spawn(async move {
                let mut ack = c.get_manual_ack(&publish);
                ack.set_reason(ManualAckReason::UnspecifiedError);
                ack.set_reason_string("Testing error".to_string().into());
                c.manual_ack(ack).await.unwrap();
                // c.ack(&publish).await.unwrap();
            });
        }
    }

    Ok(())
}

async fn requests(client: &AsyncClient) {
    for i in 1..=5 {
        client
            .publish("hello/world", QoS::AtLeastOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }
}
