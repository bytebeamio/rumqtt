use rumqttc::v5::mqttbytes::{
    v5::{ConnectProperties, PublishProperties},
    QoS,
};
use tokio::{task, time};

use rumqttc::v5::{AsyncClient, MqttOptions};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let mut conn_props = ConnectProperties::new();
    conn_props.set_topic_alias_max(10.into());

    let mut mqttoptions = MqttOptions::new("test-1", "broker.emqx.io", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_connect_properties(conn_props);

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
        .subscribe("hello/world", QoS::AtMostOnce, None)
        .await
        .unwrap();

    client
        .subscribe("bye/world", QoS::AtMostOnce, None)
        .await
        .unwrap();

    let mut props = PublishProperties::default();
    props.topic_alias = Some(28);

    client
        .publish(
            "hello/world",
            QoS::ExactlyOnce,
            false,
            vec![3; 3],
            Some(props.clone()),
        )
        .await
        .unwrap();

    let mut other = props.clone();
    other.topic_alias = Some(51);

    client
        .publish(
            "bye/world",
            QoS::ExactlyOnce,
            false,
            vec![3; 3],
            Some(other.clone()),
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
            .publish("", QoS::ExactlyOnce, false, vec![1; i], Some(properties))
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}
