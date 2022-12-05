use std::time::Duration;
use std::{error::Error, thread};

use rumqttc::v5::{mqttbytes::QoS, sync::Client, MqttOptions};

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    let mut mqttoptions = MqttOptions::new("test-1", "broker.emqx.io", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(|| requests(client));

    while let Ok(notification) = connection.recv() {
        println!("{:?}", notification);
    }

    Ok(())
}

fn requests(client: Client) {
    client.subscribe("hello/world", QoS::AtMostOnce).unwrap();

    client.subscribe("bye/world", QoS::AtMostOnce).unwrap();

    let mut publisher1 = client.publisher("hello/world").topic_alias(3);

    publisher1
        .publish(QoS::ExactlyOnce, false, vec![3; 3])
        .unwrap();

    let mut publisher2 = client.publisher("bye/world").topic_alias(5);

    publisher2
        .publish(QoS::ExactlyOnce, false, vec![3; 3])
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
            .unwrap();

        thread::sleep(Duration::from_secs(1));
    }

    thread::sleep(Duration::from_secs(120));
}
