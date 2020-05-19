use rumq_client::{self, MqttOptions, Publish, QoS, Request, Subscribe};
use std::time::Duration;
use std::thread;

fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    // Set your MQTT options
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));

    // Create a new client and extract handles to communicate with it
    let client = rumq_client::Client::new(mqttoptions, 10);
    let (tx, rx) = client.channel();
    let cancel = client.cancel_handle();

    // Start the client in a separate thread to unblock the current one. You might as well have
    // sent handles to a different thread and start client in this thread
    thread::spawn(move || {
        let mut client = client;
        client.start();
    });

    // Start a new thread to parallely send requests
    thread::spawn(move || {
        let subscription = Subscribe::new("hello/world", QoS::AtLeastOnce);
        let _ = tx.send(Request::Subscribe(subscription));
        for i in 0..100 {
            tx.send(publish_request(i)).unwrap();
            thread::sleep(Duration::from_secs(1));
        }

        thread::sleep(Duration::from_secs(3));
    });

    for (i, notification) in rx.iter().enumerate() {
        // use the cancel handle to stop the client eventloop
        if i == 10 {
            cancel.send(());
        }

        println!("Received = {:?}", notification);
    }
}

fn publish_request(i: u8) -> Request {
    let topic = "hello/world".to_owned();
    let payload = vec![1, 2, 3, i];

    let publish = Publish::new(&topic, QoS::AtLeastOnce, payload);
    Request::Publish(publish)
}
