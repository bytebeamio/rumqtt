use tokio::{task, time};

use rumqttc::{self, Event, EventLoop, Incoming, MqttOptions, Publish, QoS, Request, Subscribe};
use std::collections::HashSet;
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    color_backtrace::install();

    let num_topics = 200;
    let options = MqttOptions::new("rumqttctest", "localhost", 1883);

    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        for i in 0..num_topics {
            let topic = format!("rumqttctest4/${}", i);
            let payload = format!("{}", i);
            let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
            publish.retain = true;
            println!("publish {:?}", publish);
            requests_tx.send(Request::Publish(publish)).await.unwrap();
        }
        requests_tx
            .send(Request::Subscribe(Subscribe::new(
                "rumqttctest4/#",
                QoS::AtLeastOnce,
            )))
            .await
            .unwrap();
        time::delay_for(Duration::from_secs(10)).await;
        requests_tx.send(Request::Disconnect).await.unwrap();
    });

    let mut seen = HashSet::new();
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                println!("incoming {:?}", publish);
                seen.insert(publish.topic);
                time::delay_for(Duration::from_millis(10)).await;
            }
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
            Ok(other) => {
                log::trace!("other: {:?}", other);
            }
        }
    }

    for i in 0..num_topics {
        let topic = format!("rumqttctest4/${}", i);
        assert!(seen.contains(&topic), "Missing {}", topic);
    }
    Ok(())
}
