use rumqttc::{self, Client, MqttOptions, QoS};
use std::thread;
use std::time::Duration;

fn main() {
    pretty_env_logger::init();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5);

    let (client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(move || publish(client));

    for (i, notification) in connection.iter().enumerate() {
        if i == 10 {
            break;
        }

        println!("Notification = {:?}", notification);
    }

    println!("Iterator 1 done!!");

    // muliple iterators continue the state (after reconnection)
    for notification in connection.iter() {
        println!("Notification = {:?}", notification);
    }

    println!("Done with the stream!!");
}

fn publish(mut client: Client) {
    client.subscribe("hello/+/world", QoS::AtMostOnce).unwrap();
    for i in 0..300 {
        let payload = vec![1; i as usize];
        let topic = format!("hello/{}/world", i);
        let qos = QoS::AtLeastOnce;

        client.publish(topic, qos, true, payload).unwrap();
    }

    thread::sleep(Duration::from_secs(1));
}
