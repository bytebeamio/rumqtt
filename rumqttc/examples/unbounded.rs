use rumqttc::{Client, MqttOptions, QoS};
use std::thread;
use std::time::Duration;

fn main() {
    let mqqt_opts = MqttOptions::new("test-1", "localhost", 1883);

    let (mut client, mut connection) = Client::new(mqqt_opts, None);
    client.subscribe("hello/rumqtt", QoS::AtMostOnce).unwrap();
    thread::spawn(move || {
        for i in 0..10 {
            client
                .publish("hello/rumqtt", QoS::AtLeastOnce, false, vec![1; i])
                .unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Iterate to poll the eventloop for connection progress
    for notification in connection.iter() {
        println!("Notification = {:?}", notification);
    }
}
