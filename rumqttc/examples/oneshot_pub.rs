use tokio::time;

use rumqttc::{self, MqttHandler, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-pub", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let handler = MqttHandler::new(mqttoptions, 10);

    let mut publisher = handler.publisher("hello/world", QoS::AtLeastOnce, false);

    publisher.qos = QoS::ExactlyOnce;
    publisher.retain = false;
    for i in 1..=10 {
        publisher.publish(vec![1; i]).await.unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
