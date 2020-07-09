use rumqttc::{self, MqttOptions, Incoming, QoS, Client};
use std::time::{Duration, Instant};
use std::error::Error;
use std::thread;

mod common;

fn main() {
    pretty_env_logger::init();
    let guard = pprof::ProfilerGuard::new(250).unwrap();
    start("rumqtt-sync", 100, 1_000_000).unwrap();
    common::profile("bench.pb", guard);
}

pub fn start(id: &str, payload_size: usize, count: usize) -> Result<() , Box<dyn Error>> {
    let mut mqttoptions = MqttOptions::new(id, "localhost", 1883);
    mqttoptions.set_keep_alive(20).set_max_request_batch(0);

    // NOTE More the inflight size, better the perf
    mqttoptions.set_inflight(100);

    let (client, mut connection) = Client::new(mqttoptions, 10);
    let payloads = generate_payloads(count, payload_size);
    thread::spawn(move || {
        let mut client = client;
        requests(payloads, &mut client);
        thread::sleep(Duration::from_secs(10));
    });

    let mut acks_count = 0;
    let start = Instant::now();
    for o in connection.iter()  {
        let (notification, _) = o?;
        let notification = match notification {
            Some(n) => n,
            None => continue
        };

        match notification {
            Incoming::PubAck(_puback) => {
                acks_count += 1;
            }
            _notification => {
                continue;
            }
        };

        if acks_count == count {
            break;
        }
    }

    let elapsed_ms = start.elapsed().as_millis();
    let throughput = acks_count as usize / elapsed_ms as usize;
    let acks_throughput = throughput * 1000;
    println!("Id = {}, Messages = {}, Payload (bytes) = {}, Throughput (messages/sec) = {}",
             id,
             count,
             payload_size,
             acks_throughput,
    );
    Ok(())
}

fn requests(payloads: Vec<Vec<u8>>, client: &mut Client) {
    for payload in payloads.into_iter() {
        if let Err(e) = client.publish("hello/world", QoS::AtLeastOnce, false, payload) {
            println!("Client error: {:?}", e);
            break;
        }
    }
}

fn generate_payloads(count: usize, payload_size: usize) -> Vec<Vec<u8>> {
    vec![vec![1; payload_size]; count]
}
