#[cfg(feature = "websocket")]
use rumqttc::{self, AsyncClient, MqttOptions, QoS, Transport};
#[cfg(feature = "websocket")]
use std::{error::Error, time::Duration};
#[cfg(feature = "websocket")]
use tokio::{task, time};

#[cfg(feature = "websocket")]
#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let mqttoptions = MqttOptions::builder()
        .broker_addr("ws://broker.mqttdashboard.com:8000/mqtt")
        // port parameter is ignored when scheme is websocket
        .port(8000)
        .client_id("clientId-aSziq39Bp3".parse().expect("invalid client id"))
        .transport(Transport::Ws)
        .build();

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
        time::sleep(Duration::from_secs(3)).await;
    });

    loop {
        let event = eventloop.poll().await;
        match event {
            Ok(ev) => println!("{:?}", ev),
            Err(err) => println!("{:?}", err),
        }
    }
}

#[cfg(feature = "websocket")]
async fn requests(client: AsyncClient) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .publish("hello/world", QoS::ExactlyOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}

#[cfg(not(feature = "websocket"))]
fn main() {
    panic!("Enable websocket feature with `--features=websocket`");
}
