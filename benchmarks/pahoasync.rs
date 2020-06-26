extern crate paho_mqtt as mqtt;

use std::time::Instant;
use std::error::Error;
use mqtt::AsyncClient;
use futures::executor::block_on;

#[tokio::main(core_threads=2)]
async fn main() {
    let _o = start("paho-rust-async", 100, 100000);
}


fn start(id: &str, payload_size: usize, count: usize) -> Result<() , Box<dyn Error>> {
    // Create a client to the specified host, no persistence
    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri("tcp://localhost:1883")
        .persistence(mqtt::PersistenceType::None)
        .finalize();

    let client = mqtt::AsyncClient::new(create_opts)?;
    let payloads = generate_payloads(count, payload_size);


    let start = Instant::now();
    requests(payloads, client);
    let elapsed_ms = start.elapsed().as_millis();
    let throughput = count as usize / elapsed_ms as usize;
    let acks_throughput = throughput * 1000;
    println!("Id = {}, Messages = {}, Payload (bytes) = {}, Throughput = {} messages/s",
             id,
             count,
             payload_size,
             acks_throughput,
    );
    Ok(())
}

fn requests(payloads: Vec<Vec<u8>>, client: AsyncClient) {
    block_on(async {
        // Connect with default options
        let conn_opts = mqtt::ConnectOptions::new();
        // Connect and wait for it to complete or fail
        client.connect(conn_opts).await.unwrap();
        for payload in payloads.into_iter() {
            let msg = mqtt::Message::new("hello/world", payload, 1);
            if let Err(e) = client.publish(msg).await {
                println!("Error sending message: {:?}", e);
                break
            }
        }
    })
}

fn generate_payloads(count: usize, payload_size: usize) -> Vec<Vec<u8>> {
    vec![vec![1; payload_size]; count]
}

