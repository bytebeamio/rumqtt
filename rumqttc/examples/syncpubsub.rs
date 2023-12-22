use rumqttc::{self, Client, Filter, LastWill, Message, MqttOptions, QoS};
use std::thread;
use std::time::Duration;

fn main() {
    pretty_env_logger::init();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    let will = LastWill::new("hello/world", "good bye", QoS::AtMostOnce, false);
    mqttoptions
        .set_keep_alive(Duration::from_secs(5))
        .set_last_will(will);

    let (client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(move || publish(client));

    for (i, notification) in connection.iter().enumerate() {
        match notification {
            Ok(notif) => {
                println!("{i}. Notification = {notif:?}");
            }
            Err(error) => {
                println!("{i}. Notification = {error:?}");
                return;
            }
        }
    }

    println!("Done with the stream!!");
}

fn publish(client: Client) {
    thread::sleep(Duration::from_secs(1));

    let filter = Filter::new("hello/+/world", QoS::AtMostOnce);
    client.subscribe(filter).unwrap();

    for i in 0..10_usize {
        let message = Message {
            topic: format!("hello/{i}/world"),
            qos: QoS::AtLeastOnce,
            payload: vec![1; i],
            retain: false,
        };

        client.publish(message).unwrap();
    }

    thread::sleep(Duration::from_secs(1));
}
