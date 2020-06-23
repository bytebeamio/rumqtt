use argh::FromArgs;
use rumqttlog::router::Data;
use rumqttlog::tracker::Tracker;
use rumqttlog::{Config, Router, RouterInMessage, RouterOutMessage, Sender};
use mqtt4bytes::*;
use std::thread;
use std::time::Duration;
use tokio::{task, select, time};
use tokio::stream::StreamExt;
use async_channel::{Receiver, bounded};

pub mod common;

#[derive(Clone, FromArgs)]
/// Reach new heights.
struct CommandLine {
    /// size of payload
    #[argh(option, short = 'b', default = "1024")]
    payload_size: usize,
    /// number of messages
    #[argh(option, short = 'n', default = "1*1000*1000")]
    message_count: usize,
    /// number of topics over which data is being sent
    #[argh(option, short = 't', default = "1")]
    topic_count: usize,
    /// maximum segment size
    #[argh(option, short = 'z', default = "1*1024*1024")]
    segment_size: usize,
    /// maximum sweep read size
    #[argh(option, short = 'y', default = "100*1024")]
    sweep_size: usize,
    /// number of subscribers
    #[argh(option, short = 's', default = "4")]
    subscriber_count: usize,
    /// number of publishers
    #[argh(option, short = 'p', default = "4")]
    publisher_count: usize,
}

#[tokio::main(core_threads = 8)]
async fn main() {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();

    let config = Config {
        id: 0,
        dir: Default::default(),
        max_segment_size: commandline.segment_size as u64,
        max_segment_count: 10000,
        routers: None,
    };

    let (router, tx) = Router::new(config);
    thread::spawn(move || start_router(router));

    let (network_tx, network_rx) = bounded(100);

    // start connection
    let router_tx = tx.clone();
    task::spawn(async move {
        connection("connection-1", network_rx, router_tx).await;
    });

    // start network publisher
    let options = commandline.clone();
    task::spawn(async move {
        let network_tx = network_tx;
        for i in 0..100 {
            let payload = vec![i as u8; options.payload_size];
            let mut packet = Publish::new("hello/mqtt", QoS::AtLeastOnce, payload);
            packet.set_pkid((i % 65000) as u16 + 1);
            network_tx.send(packet).await.unwrap();
        }
    });

    time::delay_for(Duration::from_secs(500)).await;
}

#[tokio::main(core_threads = 1)]
async fn start_router(mut router: Router) {
    router.start().await;
}

async fn connection(
    id: &str,
    mut network_rx: Receiver<Publish>,
    router_tx: Sender<(usize, RouterInMessage)>
) {
    let (id, mut this_rx) = common::new_connection(id, 100, &router_tx).await;
    let mut tracker = Tracker::new();
    let mut got_last_reply = true;

    loop {
        if got_last_reply {
            if let Some(message) = tracker.next() {
                router_tx.send((id, message)).await.unwrap();
                got_last_reply = false;
            }
        }

        select! {
            Some(message) = this_rx.next() => {
                match message {
                    RouterOutMessage::DataAck(_ack) => continue,
                    packet => {
                        println!("Expecting ack. Received = {:?}", packet);
                        got_last_reply = false;
                    }
                }
            }
            Some(publish) = network_rx.next() => {
                let data = Data { topic: publish.topic, pkid: publish.pkid as u64, payload: publish.payload };
                let message = (id, RouterInMessage::Data(data));
                router_tx.send(message).await.unwrap();
            }
        }
    }
}

