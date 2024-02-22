use tokio::{
    task::{self, JoinSet},
    time,
};

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
        requests(client).await;
    });

    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(v) => {
                println!("Event = {v:?}");
            }
            Err(e) => {
                println!("Error = {e:?}");
                return Ok(());
            }
        }
    }
}

async fn requests(client: AsyncClient) {
    let mut joins = JoinSet::new();
    joins.spawn(
        client
            .subscribe("hello/world", QoS::AtMostOnce)
            .await
            .unwrap(),
    );

    for i in 1..=10 {
        joins.spawn(
            client
                .publish("hello/world", QoS::ExactlyOnce, false, vec![1; i])
                .await
                .unwrap(),
        );

        time::sleep(Duration::from_secs(1)).await;
    }

    // TODO: maybe rewrite to showcase in-between resolutions?
    while let Some(Ok(Ok(pkid))) = joins.join_next().await {
        println!("Pkid: {:?}", pkid);
    }
}
