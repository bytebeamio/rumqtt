use rumqttc::*;
use std::error::Error;
use std::time::{Duration, Instant};

use tokio::task;
use tokio::time;

mod common;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main(core_threads = 2)]
async fn main() {
    // pretty_env_logger::init();
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    start("rumqtt-async-qos0", 100, 1_000_000).await.unwrap();
    common::profile("bench.pb", guard);
}

pub async fn start(id: &str, payload_size: usize, count: usize) -> Result<(), Box<dyn Error>> {
    let mut mqttoptions = MqttOptions::new(id, "localhost", 1883);
    mqttoptions.set_keep_alive(20);
    mqttoptions.set_inflight(100);
    mqttoptions.set_max_request_batch(10);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        for _i in 0..count {
            let payload = vec![0; payload_size];
            let qos = QoS::AtMostOnce;
            client
                .publish("hello/benchmarks/world", qos, false, payload)
                .await
                .unwrap();
        }

        let qos = QoS::AtLeastOnce;
        let payload = vec![0; payload_size];
        client
            .publish("hello/benchmarks/world", qos, false, payload)
            .await
            .unwrap();
        time::delay_for(Duration::from_secs(10)).await;
    });

    let start = Instant::now();
    loop {
        if let Event::Incoming(Incoming::PubAck(_)) = eventloop.poll().await? {
            break;
        }
    }

    let elapsed_ms = start.elapsed().as_millis();
    let throughput = count as usize / elapsed_ms as usize;
    let throughput = throughput * 1000;
    println!(
        "Id = {}, Messages = {}, Payload (bytes) = {}, Throughput (messages/sec) = {}",
        id, count, payload_size, throughput,
    );

    Ok(())
}
