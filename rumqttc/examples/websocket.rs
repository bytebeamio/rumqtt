use std::error::Error;

#[cfg(feature = "websocket")]
mod example {
    use super::*;
    use rumqttc::{self, AsyncClient, MqttOptions, QoS, Transport};
    use std::time::Duration;
    use tokio::{task, time};

    pub async fn run() -> Result<(), Box<dyn Error>> {
        pretty_env_logger::init();

        // port parameter is ignored when scheme is websocket
        let mut mqttoptions = MqttOptions::new(
            "clientId-aSziq39Bp3",
            "ws://broker.mqttdashboard.com:8000/mqtt",
            8000,
        );
        mqttoptions.set_transport(Transport::Ws);
        mqttoptions.set_keep_alive(Duration::from_secs(60));

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
}

#[cfg(not(feature = "websocket"))]
mod example {
    use super::*;

    pub async fn run() -> Result<(), Box<dyn Error>> {
        panic!("Enable websocket feature with `--features=websocket`");
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    example::run().await
}
