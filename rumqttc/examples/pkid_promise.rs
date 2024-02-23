use tokio::{
    task::{self, JoinSet},
    select
};
use tokio_util::time::DelayQueue;
use futures_util::stream::StreamExt;

use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "broker.emqx.io", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
    });

    loop {
        let event = eventloop.poll().await;
        match &event {
            Ok(v) => {
                println!("Event = {v:?}");
            }
            Err(e) => {
                println!("Error = {e:?}");
                return Ok(());
            }
        }
    }
}

async fn requests(client: AsyncClient) {
    let mut joins = JoinSet::new();
    joins.spawn(
        client
            .subscribe("hello/world", QoS::AtMostOnce)
            .await
            .unwrap(),
    );

    let mut queue = DelayQueue::new();
    for i in 1..=10 {
        queue.insert(i as usize, Duration::from_secs(i));
    }

    loop {
        select!{
            Some(i) = queue.next() => {
                joins.spawn(
                    client
                        .publish("hello/world", QoS::ExactlyOnce, false, vec![1; i.into_inner()])
                        .await
                        .unwrap(),
                );
            }
            Some(Ok(Ok(pkid))) = joins.join_next() => {
                println!("Pkid: {:?}", pkid);
            }
            else => break,
        }
    }
}
