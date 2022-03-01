use rumqttc::{self, MqttHandler, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-sub", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let handler = MqttHandler::new(mqttoptions, 10);
    let mut subscriber = handler.subscriber("hello/world", QoS::AtMostOnce).await?;

    loop {
        let publish = subscriber.next().await?;
        println!("{:?}", publish);
        if publish.payload.len() == 10 {
            subscriber.unsubscribe().await?;
            return Ok(());
        }
    }
}
