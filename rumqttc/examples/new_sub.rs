use tokio::task;

use rumqttc::{self, MqttOptions, QoS, ReqHandler};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-sub", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut sub_handler) = ReqHandler::new(mqttoptions, 10);
    task::spawn(async move {
        sub_handler.start().await.unwrap();
    });

    let mut subscriber = client.subscriber("hello/world", QoS::AtMostOnce).await?;

    loop {
        let publish = subscriber.next().await?;
        println!("{:?}", publish);
        if publish.topic.contains("10") {
            subscriber.unsubscribe().await?;
        }
    }
}
