use rumq_client::{self, MqttOptions, QoS, Client};
use std::time::Duration;
use std::thread;


fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));

    let (mut client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(move || {
        client.subscribe("hello/world", QoS::AtMostOnce).unwrap();
        for i in 0..10 {
            let payload = vec![1, 2, 3, i];
            client.publish("hello", QoS::AtLeastOnce, false, payload).unwrap();
        }
    });

    for notification in connection.iter() {
        println!("{:?}", notification);
    }

    println!("Done with the stream!!");
}