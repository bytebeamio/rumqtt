use rumqttc::v5::mqttbytes::v5::{Filter, SubscribeProperties};
use rumqttc::v5::mqttbytes::QoS;
use tokio::{task, time};

use rumqttc::v5::{AsyncClient, Message, MqttOptions};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1884);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

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
    let props = SubscribeProperties {
        id: Some(1),
        user_properties: vec![],
    };

    let filter = Filter::new("hello/world", QoS::AtMostOnce);
    client
        .subscribe_with_properties(filter, props)
        .await
        .unwrap();

    let props = SubscribeProperties {
        id: Some(2),
        user_properties: vec![],
    };

    let filter = Filter::new("hello/#", QoS::AtMostOnce);
    client
        .subscribe_with_properties(filter.clone(), props)
        .await
        .unwrap();

    time::sleep(Duration::from_millis(500)).await;
    // we will receive two publishes
    // one due to hello/world and other due to hello/#
    // both will have respective subscription ids
    let mut message = Message::new("hello/world", QoS::AtMostOnce);
    message.payload = "both having subscription IDs!".into();
    client.publish(message.clone()).await.unwrap();

    time::sleep(Duration::from_millis(500)).await;
    client.unsubscribe("hello/#").await.unwrap();
    client.subscribe(filter).await.unwrap();
    time::sleep(Duration::from_millis(500)).await;

    // we will receive two publishes
    // but only one will have subscription ID
    // cuz we unsubscribed to hello/# and then
    // subscribed without properties!
    message.payload = "Only one with subscription ID!".into();
    client.publish(message).await.unwrap();
}
