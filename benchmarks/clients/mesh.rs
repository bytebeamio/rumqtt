use rumqttlog::{Config, Router, Sender, RouterInMessage, RouterOutMessage, DataRequest};
use tokio::task;
use std::thread;
use std::path::PathBuf;
use bytes::Bytes;
use rumqttlog::router::{Data};

mod common;

#[tokio::main(core_threads = 1)]
async fn main() {
    pretty_env_logger::init();

    let config = Config {
        id: 0,
        dir: PathBuf::from("/tmp/rumqttlog"),
        max_segment_size: 1024 * 1024,
        max_segment_count: 10000,
        routers: None,
    };

    let (router, tx) = Router::new(config);
    let tx_writer = tx.clone();
    thread::spawn(move || start_router(router));
    task::spawn(async move {
        write(tx_writer).await;
    });
    read(tx).await;
}

#[tokio::main(core_threads = 1)]
async fn start_router(mut router: Router) {
    router.start().await;
}

async fn write(router_tx: Sender<(usize, RouterInMessage)>) {
    let (id, _this_rx) = common::new_connection("reader", 100, &router_tx).await;
    let data = vec![Bytes::from(vec![1u8; 1024]); 10];
    for payload in data.into_iter() {
        let data = Data { topic: "hello/world".to_owned(), pkid: 0, payload, };
        let message = (id, RouterInMessage::Data(data));
        router_tx.send(message).await.unwrap();
    }
}

async fn read(tx: Sender<(usize, RouterInMessage)>) {
    let (id, this_rx) = common::new_connection("reader", 100, &tx).await;

    let payload_size = 1024;
    let mut total_size = 0;
    let mut offset = 0;
    let mut segment = 0;

    for _ in 0..10 {
        let request = DataRequest {
            topic: "hello/world".to_owned(),
            native_segment: segment,
            native_offset: offset,
            replica_segment: 0,
            replica_offset: 0,
            size: 10 * 1024,
            tracker_topic_offset: 0,
        };

        let message = (id.to_owned(), RouterInMessage::DataRequest(request));
        tx.send(message).await.unwrap();
        if let RouterOutMessage::DataReply(data_reply) = this_rx.recv().await.unwrap() {
            println!("{:?}, {:?}" ,data_reply.topic, data_reply.payload.len());
            segment = data_reply.native_segment;
            offset = data_reply.native_offset + 1;
            total_size += data_reply.payload.len() * payload_size;
        }
    }

    println!("Id = {}, Total size = {}", id, total_size);
}


