use rumqttc::{self, Client, LastWill, MqttOptions, QoS};
use std::thread;
use std::time::Duration;

fn main() {
    pretty_env_logger::init();

    let will = LastWill::new("hello/world", "good bye", QoS::AtMostOnce, false);
    let mqttoptions = MqttOptions::builder()
        .broker_addr("localhost")
        .port(1883)
        .client_id("test-1".parse().expect("invalid client id"))
        .last_will(will)
        .keep_alive(Duration::from_secs(5))
        .build();

    let (client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(move || publish(client));

    for (i, notification) in connection.iter().enumerate() {
        println!("{}. Notification = {:?}", i, notification);
    }

    println!("Done with the stream!!");
}

fn publish(mut client: Client) {
    client.subscribe("hello/+/world", QoS::AtMostOnce).unwrap();
    for i in 0..10_usize {
        let payload = vec![1; i];
        let topic = format!("hello/{}/world", i);
        let qos = QoS::AtLeastOnce;

        client.publish(topic, qos, true, payload).unwrap();
    }

    thread::sleep(Duration::from_secs(1));
}
