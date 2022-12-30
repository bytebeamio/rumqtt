use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{unsync::EventLoop, MqttOptions};
use std::error::Error;

use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1884);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let mut eventloop = EventLoop::new(mqttoptions, 10);

    eventloop
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    for i in 1..=10 {
        eventloop
            .publish("hello/world", QoS::AtMostOnce, false, vec![1; i])
            .await
            .unwrap();
    }

    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(v) => {
                println!("Event = {:?}", v);
            }
            Err(e) => {
                println!("Error = {:?}", e);
                return Ok(());
            }
        }
    }
}
