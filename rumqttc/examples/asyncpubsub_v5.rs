use rumqttc::v5::mqttbytes::QoS;
use tokio::sync::Mutex;
use tokio::{task, time};

use rumqttc::v5::{unsync::EventLoop, MqttOptions};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1884);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let eventloop = Arc::new(Mutex::new(EventLoop::new(mqttoptions, 10)));
    let cloned = eventloop.clone();
    task::spawn(async move {
        requests(cloned).await;
        time::sleep(Duration::from_secs(3)).await;
    });

    while let Ok(event) = dbg!(eventloop.lock().await.poll().await) {
        dbg!("sad");
        println!("{:?}", event);
    }

    Ok(())
}

async fn requests(eventloop: Arc<Mutex<EventLoop>>) {
    let mut eventloop = eventloop.lock().await;
    eventloop
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    for i in 1..=10 {
        eventloop
            .publish("hello/world", QoS::ExactlyOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}
