use rumq_client::{self, EventLoop, MqttOptions, Incoming, QoS, Request};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use std::error::Error;

use tokio::select;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

#[derive(Debug, Clone)]
pub struct Metrics {
    progress: u16,
}

#[tokio::main(core_threads = 1)]
async fn main() {
    pretty_env_logger::init();
    let o = start("rumq-async", 100, 1000).await;
    println!("\n\nDone!! Result = {:?}", o);
}

pub async fn start(id: &str, payload_size: usize, count: u16) -> Result<() , Box<dyn Error>> {
    let (requests_tx, requests_rx) = channel(10);
    let mut mqttoptions = MqttOptions::new(id, "localhost", 1883);
    mqttoptions.set_keep_alive(5);

    // NOTE More the inflight size, better the perf
    mqttoptions.set_inflight(200);

    let mut eventloop = EventLoop::new(mqttoptions, requests_rx).await;
    let client_id = id.to_owned();
    task::spawn(async move {
        requests(&client_id, payload_size, count, requests_tx).await;
        time::delay_for(Duration::from_secs(10)).await;
    });

    eventloop.connect().await.unwrap();
    let mut acks = acklist(count);
    let mut incoming = acklist(count);
    let mut interval = time::interval(Duration::from_secs(1));
    let mut data = Metrics {
        progress: 0,
    };

    let start = Instant::now();
    let mut acks_elapsed_ms = 0;

    loop {
        let (notification, _) = eventloop.poll().await?;
        let notification = match notification {
            Some(n) => n,
            None => continue
        };

        match notification {
            Incoming::Puback(puback) => {
                acks.remove(&puback.pkid);
            }
            Incoming::Suback(suback) => {
                acks.remove(&suback.pkid);
                suback.pkid;
            }
            Incoming::Publish(publish) => {
                data.progress = publish.pkid;
                incoming.remove(&publish.pkid);
            }
            notification => {
                println!("Id = {}, Incoming = {:?}", id, notification);
                continue;
            }
        };

        if acks.len() == 0 {
            break;
        }
    }

    let elapsed_ms = start.elapsed().as_millis();
    let acks_count = count - acks.len() as u16;
    let total_outgoing_size = payload_size * acks_count as usize;
    let acks_throughput = total_outgoing_size / elapsed_ms as usize;
    let acks_throughput_mbps = acks_throughput * 1000 / 1024;

    println!(
        "Id = {},
        Acks:     Missed = {:<5}, Received size = {}, Incoming Throughput = {} KB/s",
        id,
        acks.len(),
        total_outgoing_size,
        acks_throughput_mbps,
    );

    Ok(())
}

async fn requests(id: &str, payload_size: usize, count: u16, mut requests_tx: Sender<Request>) {
    let topic = format!("hello/{}/world", id);
    // let subscription = rumq_client::Subscribe::new(&topic, QoS::AtLeastOnce);
    // let _ = requests_tx.send(Request::Subscribe(subscription)).await;

    for i in 0..count {
        let mut payload = generate_payload(payload_size);
        payload[0] = (i % 255) as u8;
        let publish = rumq_client::Publish::new(&topic, QoS::AtLeastOnce, payload);
        let publish = Request::Publish(publish);
        if let Err(_) = requests_tx.send(publish).await {
            break;
        }
    }

    time::delay_for(Duration::from_secs(5)).await;
}

fn acklist(count: u16) -> HashSet<u16> {
    let mut acks = HashSet::new();
    for i in 1..=count {
        acks.insert(i);
    }

    acks
}

fn generate_payload(payload_size: usize) -> Vec<u8> {
    vec![1; payload_size]
}
