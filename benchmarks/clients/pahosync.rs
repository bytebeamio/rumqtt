extern crate paho_mqtt as mqtt;

use std::time::{Duration, Instant};
use std::error::Error;
use mqtt::Client;

fn main() {
    let _o = start("paho-rust", 100, 100_000);
}


pub fn start(id: &str, payload_size: usize, count: usize) -> Result<() , Box<dyn Error>> {
    // Create a client & define connect options
    let mut client = mqtt::Client::new("tcp://localhost:1883").unwrap();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(5))
        .clean_session(true)
        .max_inflight(1000)
        .finalize();

    // Connect and wait for it to complete or fail
    client.connect(conn_opts)?;
    let payloads = generate_payloads(count, payload_size);


    let start = Instant::now();
    requests(payloads, &mut client);
    let elapsed_ms = start.elapsed().as_millis();
    let throughput = count as usize / elapsed_ms as usize;
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
        let msg = mqtt::Message::new("hello/world", payload, 1);
        if let Err(e) = client.publish(msg) {
            println!("Error sending message: {:?}", e);
            break
        }
    }
}

fn generate_payloads(count: usize, payload_size: usize) -> Vec<Vec<u8>> {
    vec![vec![1; payload_size]; count]
}

