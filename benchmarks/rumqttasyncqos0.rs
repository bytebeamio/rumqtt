use rumqttc::{self, EventLoop, Incoming, MqttOptions, PublishRaw, QoS, Request};
use std::error::Error;
use std::time::{Duration, Instant};

use async_channel::Sender;
use tokio::task;
use tokio::time;

mod common;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main(core_threads = 2)]
async fn main() {
    pretty_env_logger::init();
    // let guard = pprof::ProfilerGuard::new(100).unwrap();
    start("rumqtt-async", 100, 1_000_000).await.unwrap();
    // common::profile("bench.pb", guard);
}

pub async fn start(id: &str, payload_size: usize, count: usize) -> Result<(), Box<dyn Error>> {
    let mut mqttoptions = MqttOptions::new(id, "localhost", 1883);
    mqttoptions.set_keep_alive(1000);
    mqttoptions.set_max_request_batch(10);

    // NOTE More the inflight size, better the perf
    mqttoptions.set_inflight(100);

    let mut eventloop = EventLoop::new(mqttoptions, 10);
    let requests_tx = eventloop.handle();
    let client_id = id.to_owned();
    let payloads = generate_payloads(count, payload_size);
    task::spawn(async move {
        requests(&client_id, payloads, requests_tx).await;
        time::delay_for(Duration::from_secs(10)).await;
    });

    let start = Instant::now();
    'main: loop {
        let (notifications, _) = eventloop.poll().await?;
        for notification in notifications {
            match notification {
                Incoming::PingResp => break 'main,
                _notification => {
                    continue;
                }
            };
        }
    }

    let elapsed_ms = start.elapsed().as_millis();
    let throughput = count as usize / elapsed_ms as usize;
    let throughput = throughput * 1000;
    println!(
        "Id = {}, Messages = {}, Payload (bytes) = {}, Throughput (messages/sec) = {}",
        id, count, payload_size, throughput
    );
    Ok(())
}

async fn requests(id: &str, payloads: Vec<Vec<u8>>, requests_tx: Sender<Request>) {
    let topic = format!("hello/{}/world", id);
    // let subscription = rumqttc::Subscribe::new(&topic, QoS::AtLeastOnce);
    // let _ = requests_tx.send(Request::Subscribe(subscription)).await;
    for payload in payloads.into_iter() {
        let publish = PublishRaw::new(&topic, QoS::AtMostOnce, payload).unwrap();
        let publish = Request::PublishRaw(publish);
        requests_tx.send(publish).await.unwrap();
    }

    let ping = Request::PingReq;
    requests_tx.send(ping).await.unwrap();
    time::delay_for(Duration::from_secs(5)).await;
}

fn generate_payloads(count: usize, payload_size: usize) -> Vec<Vec<u8>> {
    vec![vec![1; payload_size]; count]
}
