use std::error::Error;
use std::time::Duration;
use tokio::{task, time};

use rumqttc::v5::{mqttbytes::QoS, unsync::Client, MqttOptions};

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let mut mqttoptions = MqttOptions::new("test-1", "broker.emqx.io", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = Client::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
        time::sleep(Duration::from_secs(3)).await;
    });

    while let Ok(event) = eventloop.poll().await {
        println!("{:?}", event);
    }

    Ok(())
}

async fn requests(client: Client) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    client
        .subscribe("bye/world", QoS::AtMostOnce)
        .await
        .unwrap();

    let mut publisher1 = client.publisher("hello/world").topic_alias(3);

    publisher1
        .publish(QoS::ExactlyOnce, false, vec![3; 3])
        .await
        .unwrap();

    let mut publisher2 = client.publisher("bye/world").topic_alias(5);

    publisher2
        .publish(QoS::ExactlyOnce, false, vec![3; 3])
        .await
        .unwrap();

    for i in 1..=10 {
        // just for choosing publisher alternately
        let publisher = if i % 2 == 0 {
            &mut publisher1
        } else {
            &mut publisher2
        };

        publisher
            .publish(QoS::ExactlyOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}
