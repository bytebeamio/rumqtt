use rumqttc::{self, Client, Event, Incoming, MqttOptions, QoS};
use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

mod common;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    pretty_env_logger::init();
    let guard = pprof::ProfilerGuard::new(250).unwrap();
    start("rumqtt-sync", 100, 1_000_000).unwrap();
    common::profile("bench.pb", guard);
}

pub fn start(id: &str, payload_size: usize, count: usize) -> Result<(), Box<dyn Error>> {
    let mut mqttoptions = MqttOptions::new(id, "localhost", 1883);
    mqttoptions.set_keep_alive(20);
    mqttoptions.set_max_request_batch(10);

    // NOTE More the inflight size, better the perf
    mqttoptions.set_inflight(100);

    let (client, mut connection) = Client::new(mqttoptions, 10);
    thread::spawn(move || {
        let mut client = client;
        requests(count, payload_size, &mut client);
        thread::sleep(Duration::from_secs(1));
    });

    let mut acks_count = 0;
    let start = Instant::now();
    for event in connection.iter() {
        if let Ok(Event::Incoming(Incoming::PubAck(_))) = event {
            acks_count += 1;

            if acks_count == count {
                break;
            }
        }
    }

    let elapsed_ms = start.elapsed().as_millis();
    let throughput = (acks_count as usize * 1000) / elapsed_ms as usize;
    println!(
        "Id = {}, Messages = {}, Payload (bytes) = {}, Throughput (messages/sec) = {}",
        id, acks_count, payload_size, throughput,
    );
    Ok(())
}

fn requests(count: usize, payload_size: usize, client: &mut Client) {
    for i in 0..count {
        let mut payload = vec![1; payload_size];
        payload[0] = (i % 255) as u8;
        if let Err(e) = client.publish("hello/world", QoS::AtLeastOnce, false, payload) {
            println!("Client error: {:?}", e);
            break;
        }
    }
}
