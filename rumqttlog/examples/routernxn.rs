use std::time::Instant;
use rumqttlog::{channel, Router, Config, RouterInMessage, Sender, DataRequest, RouterOutMessage};
use argh::FromArgs;
use mqtt4bytes::*;
use std::thread;
use rumqttlog::router::Connection;
use bytes::BytesMut;
use futures_util::future::join_all;
use tokio::task;

pub mod common;

#[derive(Clone, FromArgs)]
/// Reach new heights.
struct CommandLine {
    /// size of payload
    #[argh(option, short = 'p', default = "1024")]
    payload_size: usize,
    /// number of messages
    #[argh(option, short = 'n', default = "1*1024*1024")]
    count: usize,
    /// maximum segment size
    #[argh(option, short = 's', default = "1*1024*1024")]
    segment_size: usize,
    /// maximum sweep read size
    #[argh(option, short = 'r', default = "100*1024")]
    sweep_size: usize,
    /// maximum sweep read size
    #[argh(option, short = 'z', default = "4")]
    subscriber_count: usize,
}

#[tokio::main(core_threads=1)]
async fn main() {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();

    let config = Config {
        id: 0,
        dir: Default::default(),
        max_segment_size: commandline.segment_size as u64,
        max_segment_count: 10000,
        routers: None
    };

    let (router, tx) = Router::new(config);
    thread::spawn(move || start_router(router));
    write(&commandline, tx.clone()).await;

    let mut reads = Vec::new();
    for i in 0..commandline.subscriber_count {
        let commandline = commandline.clone();
        let tx = tx.clone();
        let f = task::spawn(async move {
            read(&commandline, &format!("device-{}", i), tx).await;
        });

        reads.push(f);
    }

    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    join_all(reads).await;
    let total_size = commandline.payload_size * commandline.count * commandline.subscriber_count;
    common::report("read.pb", total_size as u64, start, guard);
}


#[tokio::main(core_threads = 1)]
async fn start_router(mut router: Router) {
    router.start().await;
}

async fn write(commandline: &CommandLine, mut tx: Sender<(String, RouterInMessage)>) {
    // 10K packets of 1K size each. 10M total data
    let data = vec![publish(commandline.payload_size); commandline.count];
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    for packet in data.into_iter() {
        let message = ("device-0".to_owned(), RouterInMessage::Packet(packet));
        tx.send(message).await.unwrap();
    }

    common::report("write.pb", (commandline.count * commandline.payload_size) as u64, start, guard);
}

async fn read(commandline: &CommandLine, id: &str, mut tx: Sender<(String, RouterInMessage)>) -> usize {
    let (this_tx, mut this_rx) = channel(100);
    let connection = Connection {
        connect: rumqttlog::mqtt4bytes::Connect::new(id),
        handle: this_tx,
    };

    let message = (id.to_owned(), RouterInMessage::Connect(connection));
    tx.send(message).await.unwrap();
    let mut offset = 0;
    let mut segment = 0;
    let count = commandline.payload_size * commandline.count / commandline.sweep_size;

    let mut total_size = 0;
    for _ in 0..count {
        let request = DataRequest {
            topic: "hello/world".to_owned(),
            segment,
            offset,
            size: commandline.sweep_size as u64
        };

        let message = (id.to_owned(), RouterInMessage::DataRequest(request));
        tx.send(message).await.unwrap();
        if let RouterOutMessage::DataReply(data_reply) = this_rx.recv().await.unwrap() {
            segment = data_reply.segment;
            offset = data_reply.offset + 1;
            total_size += data_reply.payload.len() * commandline.payload_size;
        }
    }

    total_size
}

pub fn publish(len: usize) -> Packet {
    let mut publish = Publish::new("hello/world", QoS::AtLeastOnce, vec![1; len]);
    publish.set_pkid(1);
    // serialize bytes
    let mut payload = BytesMut::new();
    let packet = Packet::Publish(publish.clone());
    mqtt_write(packet, &mut payload).unwrap();
    publish.bytes = payload.freeze();
    Packet::Publish(publish)
}

