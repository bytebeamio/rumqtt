use tokio::task::{self, JoinSet};

use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

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
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap()
        .wait_async()
        .await
        .unwrap();

    // Publish and spawn wait for notification
    let mut set = JoinSet::new();

    let future = client
        .publish("hello/world", QoS::AtMostOnce, false, vec![1; 1024])
        .await
        .unwrap();
    set.spawn(future.wait_async());

    let future = client
        .publish("hello/world", QoS::AtLeastOnce, false, vec![1; 1024])
        .await
        .unwrap();
    set.spawn(future.wait_async());

    let future = client
        .publish("hello/world", QoS::ExactlyOnce, false, vec![1; 1024])
        .await
        .unwrap();
    set.spawn(future.wait_async());

    while let Some(res) = set.join_next().await {
        println!("Acknoledged = {:?}", res?);
    }

    Ok(())
}
