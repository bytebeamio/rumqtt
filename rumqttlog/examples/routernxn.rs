/*
use argh::FromArgs;
use bytes::Bytes;
use futures_util::future::join_all;
use rumqttlog::router::{Connection, ConnectionAck, Data};
use rumqttlog::{bounded, Config, DataRequest, Router, RouterInMessage, RouterOutMessage, Sender};
use std::thread;
use std::time::Instant;
use tokio::task;

pub mod common;

#[derive(Clone, FromArgs)]
/// Reach new heights.
struct CommandLine {
    /// size of payload
    #[argh(option, short = 'p', default = "1024")]
    payload_size: usize,
    /// number of messages
    #[argh(option, short = 'n', default = "1*1000*1000")]
    message_count: usize,
    /// number of topics over which messages are distributed over
    #[argh(option, short = 't', default = "1")]
    topic_count: usize,
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

#[tokio::main(core_threads = 1)]
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
    write(&commandline, tx.clone()).await;

    let mut reads = Vec::new();
    for i in 100..commandline.subscriber_count + 100 {
        let commandline = commandline.clone();
        let tx = tx.clone();
        let f = task::spawn(async move {
            read(&commandline, i, tx).await;
        });

        reads.push(f);
    }

    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    join_all(reads).await;
    let total_size =
        commandline.payload_size * commandline.message_count * commandline.subscriber_count;
    common::report("read.pb", total_size as u64, start, guard);
}

#[tokio::main(core_threads = 1)]
async fn start_router(mut router: Router) {
    router.start().await;
}

async fn write(commandline: &CommandLine, tx: Sender<(usize, RouterInMessage)>) {
    // 10K packets of 1K size each. 10M total data
    let data = vec![Bytes::from(vec![1u8; commandline.payload_size]); commandline.message_count];
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();

    let (connection, this_rx) = Connection::new("writer-1", 1000);
    let message = (0, RouterInMessage::Connect(connection));
    tx.send(message).await.unwrap();

    let id = match this_rx.recv().await.unwrap() {
        RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
        RouterOutMessage::ConnectionAck(ConnectionAck::Failure(e)) => {
            panic!("Connection failed {:?}", e)
        }
        message => panic!("Not connection ack = {:?}", message),
    };

    let mut topic_count = 0;
    let mut topic = "hello/world".to_owned() + &topic_count.to_string();
    for (i, payload) in data.into_iter().enumerate() {
        let data = Data {
            topic: topic.clone(),
            pkid: 0,
            payload,
        };
        let message = (id, RouterInMessage::Data(data));
        tx.send(message).await.unwrap();

        // Listen for acks in batches to fix send/recv synchronization stalls
        let count = i + 1;
        if count == commandline.message_count / commandline.topic_count {
            topic_count += 1;
            topic = "hello/world".to_owned() + &topic_count.to_string();
        }
    }
    common::report(
        "write.pb",
        (commandline.message_count * commandline.payload_size) as u64,
        start,
        guard,
    );
}

async fn read(commandline: &CommandLine, id: usize, tx: Sender<(usize, RouterInMessage)>) -> usize {
    let (connection, this_rx) = Connection::new("writer-1", 1000);
    let message = (id, RouterInMessage::Connect(connection));
    tx.send(message).await.unwrap();
    let id = match this_rx.recv().await.unwrap() {
        RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
        RouterOutMessage::ConnectionAck(ConnectionAck::Failure(e)) => {
            panic!("Connection failed {:?}", e)
        }
        message => panic!("Not connection ack = {:?}", message),
    };

    let mut offset = 0;
    let mut segment = 0;
    let count = commandline.payload_size * commandline.message_count / commandline.sweep_size;

    let mut topic_count = 0;
    let mut topic = "hello/world".to_owned() + &topic_count.to_string();
    let mut total_size = 0;
    for _ in 0..count {
        let request = DataRequest {
            topic: topic.clone(),
            native_segment: segment,
            native_offset: offset,
            replica_segment: 0,
            replica_offset: 0,
            size: commandline.sweep_size as u64,
        };

        let message = (id.to_owned(), RouterInMessage::DataRequest(request));
        tx.send(message).await.unwrap();
        if let RouterOutMessage::DataReply(data_reply) = this_rx.recv().await.unwrap() {
            segment = data_reply.native_segment;
            offset = data_reply.native_offset + 1;
            total_size += data_reply.payload.len() * commandline.payload_size;
        }

        if count == commandline.message_count / commandline.topic_count {
            topic_count += 1;
            topic = "hello/world".to_owned() + &topic_count.to_string();
        }
    }

    println!("Id = {}, Total size = {}", id, total_size);
    total_size
}
*/

fn main() {
    println!("hello world!!");
}
