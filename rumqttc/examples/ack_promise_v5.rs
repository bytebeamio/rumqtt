use tokio::task::{self, JoinSet};

use rumqttc::v5::{mqttbytes::QoS, AsyncClient, MqttOptions};
use std::error::Error;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match &event {
                Ok(v) => {
                    println!("Event = {v:?}");
                }
                Err(e) => {
                    println!("Error = {e:?}");
                }
            }
        }
    });

    // Subscribe and wait for broker acknowledgement
    match client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap()
        .await
    {
        Ok(pkid) => println!("Acknowledged Sub({pkid:?})"),
        Err(e) => println!("Subscription failed: {e:?}"),
    }

    // Publish at all QoS levels and wait for broker acknowledgement
    for (i, qos) in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce]
        .into_iter()
        .enumerate()
    {
        match client
            .publish("hello/world", qos, false, vec![1; i])
            .await
            .unwrap()
            .await
        {
            Ok(pkid) => println!("Acknowledged Pub({pkid:?})"),
            Err(e) => println!("Publish failed: {e:?}"),
        }
    }

    // Publish with different QoS levels and spawn wait for notification
    let mut set = JoinSet::new();
    for (i, qos) in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce]
        .into_iter()
        .enumerate()
    {
        let token = client
            .publish("hello/world", qos, false, vec![1; i])
            .await
            .unwrap();
        set.spawn(token);
    }

    while let Some(Ok(res)) = set.join_next().await {
        match res {
            Ok(pkid) => println!("Acknowledged Pub({pkid:?})"),
            Err(e) => println!("Publish failed: {e:?}"),
        }
    }

    // Unsubscribe and wait for broker acknowledgement
    match client.unsubscribe("hello/world").await.unwrap().await {
        Ok(pkid) => println!("Acknowledged Unsub({pkid:?})"),
        Err(e) => println!("Unsubscription failed: {e:?}"),
    }

    Ok(())
}
