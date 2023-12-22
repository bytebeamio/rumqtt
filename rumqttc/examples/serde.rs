use bincode::ErrorKind;
use rumqttc::{Client, Event, Filter, Incoming, Message, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::thread;
use std::time::{Duration, SystemTime};

#[derive(Serialize, Deserialize, Debug)]
struct PayloadMessage {
    i: usize,
    time: SystemTime,
}

impl From<&PayloadMessage> for Vec<u8> {
    fn from(value: &PayloadMessage) -> Self {
        bincode::serialize(value).unwrap()
    }
}

impl From<PayloadMessage> for Vec<u8> {
    fn from(value: PayloadMessage) -> Self {
        bincode::serialize(&value).unwrap()
    }
}

impl TryFrom<&[u8]> for PayloadMessage {
    type Error = Box<ErrorKind>;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(value)
    }
}

fn main() {
    let mqqt_opts = MqttOptions::new("test-1", "localhost", 1883);

    let (client, mut connection) = Client::new(mqqt_opts, 10);

    let filter = Filter::new("hello/rumqtt", QoS::AtMostOnce);
    client.subscribe(filter).unwrap();

    thread::spawn(move || {
        let mut pub_message = Message::new("hello/rumqtt", QoS::AtLeastOnce);
        for i in 0..10 {
            let message = PayloadMessage {
                i,
                time: SystemTime::now(),
            };

            pub_message.payload = message.into();

            client.publish(pub_message.clone()).unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Iterate to poll the eventloop for connection progress
    for notification in connection.iter().flatten() {
        if let Event::Incoming(Incoming::Publish(packet)) = notification {
            match PayloadMessage::try_from(packet.payload.as_ref()) {
                Ok(message) => println!("Payload = {message:?}"),
                Err(error) => println!("Error = {error}"),
            }
        }
    }
}
