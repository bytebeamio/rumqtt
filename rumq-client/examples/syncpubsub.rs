use rumq_client::{self, MqttOptions, QoS};
use std::time::Duration;
use std::thread;

fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    // Set your MQTT options
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));

    // Create a new client and extract handles to communicate with it
    let (mut client, connection) = rumq_client::Client::new(mqttoptions, 10);
    let notifications = client.notifications();

    // Start the client in a separate thread to unblock the current one. You might as well have
    // sent handles to a different thread and start client in this thread
    thread::spawn(move || {
        let mut connection = connection;
        connection.start();
    });

    // Start a new thread to send requests
    let mut publish_client = client.clone();
    thread::spawn(move || {
        publish_client.subscribe("hello/world", QoS::AtLeastOnce).unwrap();
        for i in 0..100 {
            publish_client.publish("hello/world", QoS::AtLeastOnce, false, vec![1, 2, 3, i]).unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });

    // Receive incoming notifications
    for (i, notification) in notifications.iter().enumerate() {
        // use the cancel handle to stop the client eventloop
        if i == 10 {
            client.cancel().unwrap();
        }

        println!("Received = {:?}", notification);
    }
}

