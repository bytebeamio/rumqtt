use rumqttc::v4::{Client, Event, MqttOptions, Packet, QoS};
use std::thread;
use std::time::Duration;

fn main() {
    let mut mqttoptions = MqttOptions::new("test1", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_inflight(5);

    let (mut client, mut connection) = Client::new(mqttoptions, 25);

    client.subscribe("test/me", QoS::AtMostOnce).unwrap();

    thread::spawn(move || publish(client));

    for notification in connection.iter() {
        println!("notification: {:?}", notification);
        match notification.unwrap() {
            Event::Incoming(inc) => match inc {
                Packet::Publish(_) => {
                    thread::sleep(Duration::from_millis(200));
                }
                _ => {}
            },
            _ => {}
        }
    }

    println!("Done with the stream!!");
}

fn publish(client: Client) {
    loop {
        let payload = b"Foobar";
        let mut cl2 = client.clone();
        cl2.publish("test/me", QoS::AtLeastOnce, false, &payload[..])
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        println!("send!");
    }
}
