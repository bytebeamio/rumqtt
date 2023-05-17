use rumqttc::v5::mqttbytes::{v5::PublishProperties, QoS};
use tokio::{task, time};

use rumqttc::v5::{AsyncClient, MqttOptions};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let mut mqttoptions = MqttOptions::new("test-1", "broker.emqx.io", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_topic_alias_max(10.into());

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
        time::sleep(Duration::from_secs(3)).await;
    });

    while let Ok(event) = eventloop.poll().await {
        println!("{:?}", event);
    }

    Ok(())
}

async fn requests(client: AsyncClient) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    client
        .subscribe("bye/world", QoS::AtMostOnce)
        .await
        .unwrap();

    let mut props = PublishProperties::default();
    props.topic_alias = Some(28);

    client
        .publish_with_properties(
            "hello/world",
            QoS::ExactlyOnce,
            false,
            vec![3; 3],
            props.clone(),
        )
        .await
        .unwrap();

    let mut other = props.clone();
    other.topic_alias = Some(51);

    client
        .publish_with_properties(
            "bye/world",
            QoS::ExactlyOnce,
            false,
            vec![3; 3],
            other.clone(),
        )
        .await
        .unwrap();

    for i in 1..=10 {
        let properties = if i % 2 == 0 {
            other.clone()
        } else {
            props.clone()
        };
        client
            .publish_with_properties("", QoS::ExactlyOnce, false, vec![1; i], properties)
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}
