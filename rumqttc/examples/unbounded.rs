use rumqttc::{Client, MqttOptions, QoS};
use std::thread;
use std::time::Duration;

fn main() {
    let mqtt_opts = MqttOptions::new("test-1", "localhost", 1883);

    // unbounded channel, no backpressure
    let (mut client, mut connection) = Client::builder(mqtt_opts).unbounded().build();

    client.subscribe("hello/rumqtt", QoS::AtMostOnce).unwrap();

    thread::spawn(move || {
        for i in 0..10 {
            client
                .publish("hello/rumqtt", QoS::AtLeastOnce, false, vec![1; i])
                .unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    });

    for notification in connection.iter() {
        println!("Notification = {:?}", notification);
    }
}
