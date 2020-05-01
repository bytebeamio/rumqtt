use rumq_client::{self, MqttOptions, Publish, QoS, Request, Subscribe, Sender};
use std::time::Duration;
use std::thread;

fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(5).set_throttle(Duration::from_secs(1));
    let (tx, rx) = rumq_client::channel(mqttoptions, 10);

    thread::spawn(move || {
        requests(tx);
        thread::sleep(Duration::from_secs(3));
    });

    for notification in rx {
        println!("Received = {:?}", notification);
    }
}

fn requests(requests_tx: Sender<Request>) {
    let subscription = Subscribe::new("hello/world", QoS::AtLeastOnce);
    let _ = requests_tx.send(Request::Subscribe(subscription));

    for i in 0..10 {
        requests_tx.send(publish_request(i)).unwrap();
        thread::sleep(Duration::from_secs(1));
    }

    thread::sleep(Duration::from_secs(10));
}

fn publish_request(i: u8) -> Request {
    let topic = "hello/world".to_owned();
    let payload = vec![1, 2, 3, i];

    let publish = Publish::new(&topic, QoS::AtLeastOnce, payload);
    Request::Publish(publish)
}
