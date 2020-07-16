/*

//! 3 node cluster for correctness and chaos testing
//!
//!

use bytes::Bytes;
use rumqttlog::router::{ConnectionAck, Data};
use rumqttlog::{
    bounded, Config, Connection, DataReply, DataRequest, Receiver, Router, RouterInMessage,
    RouterOutMessage, Sender,
};
use std::path::PathBuf;
use std::thread;
use tokio::task;
use tokio::time::Duration;

// Configuration parameters

/// Number of messages sent by a connection to node 1
const COUNT: u64 = 10;

type ConnectionId = usize;

#[test]
fn three_node_mesh() {
    thread::spawn(move || {
        node_1();
    });

    thread::sleep(Duration::from_secs(10));
}

// TODO not workin with core_threads = 1. Investigate
#[tokio::main(core_threads = 2)]
async fn node_1() {
    let config = config(1);
    let (router, router_tx) = Router::new(config);

    task::spawn(async move {
        let mut router = router;
        router.start().await
    });

    // Register, send data and receive acks
    let (id, this_rx) = register_with_router(100, "connection-1", &router_tx).await;
    task::spawn(async move {
        for i in 1..=COUNT {
            receive_ack(&this_rx, i).await;
        }
    });

    for pkid in 1..=COUNT {
        send_data(id, &router_tx, 10, pkid).await;
    }
}

#[tokio::main(core_threads = 2)]
async fn node_2() {
    let config = config(2);
    let (router, router_tx) = Router::new(config);

    task::spawn(async move {
        let mut router = router;
        router.start().await
    });

    let (_id, this_rx) = register_with_router(100, "connection-2", &router_tx).await;
    // incoming data from the router
    loop {
        let _reply = wait_for_new_data(&this_rx).await;
    }
}

/// Everything from here is a list of helper methods to make the test readable
/// ---------------------------------------------------------------------------
///

async fn receive_ack(this_rx: &Receiver<RouterOutMessage>, pkid: u64) {
    match this_rx.recv().await.unwrap() {
        RouterOutMessage::DataAck(ack) => {
            assert_eq!(ack.pkid, pkid);
            assert_eq!(ack.offset, pkid);
        }
        message => panic!("Invalid message = {:?}", message),
    };
}

async fn wait_for_new_data(rx: &Receiver<RouterOutMessage>) -> DataReply {
    tokio::time::delay_for(Duration::from_secs(1)).await;
    match rx.try_recv() {
        Ok(RouterOutMessage::DataReply(reply)) => reply,
        v => panic!("Expecting Data Reply. {:?}", v),
    }
}

fn ask_data(
    id: usize,
    router_tx: &Sender<(ConnectionId, RouterInMessage)>,
    topic: &str,
    native_offset: u64,
    replica_offset: u64,
) {
    let message = (
        id,
        RouterInMessage::DataRequest(DataRequest {
            topic: topic.to_string(),
            native_segment: 0,
            replica_segment: 0,
            native_offset,
            replica_offset,
            size: 100 * 1024,
        }),
    );

    router_tx.try_send(message).unwrap();
}

async fn send_data(
    id: ConnectionId,
    router_tx: &Sender<(ConnectionId, RouterInMessage)>,
    payload_size: usize,
    pkid: u16,
) {
    let payload = vec![pkid as u8; payload_size];
    let payload = Bytes::from(payload);
    let topic = "hello/distributed/broker".to_owned();
    let message = RouterInMessage::Data(Data {
        topic,
        pkid,
        payload,
    });
    router_tx.send((id, message)).await.unwrap();
}

async fn register_with_router(
    cap: usize,
    id: &str,
    router_tx: &Sender<(ConnectionId, RouterInMessage)>,
) -> (ConnectionId, Receiver<RouterOutMessage>) {
    let (connection, this_rx) = Connection::new(id, cap);
    let message = (0, RouterInMessage::Connect(connection));
    router_tx.send(message).await.unwrap();

    let id = match this_rx.recv().await.unwrap() {
        RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
        RouterOutMessage::ConnectionAck(ConnectionAck::Failure(e)) => {
            panic!("Connection failed {:?}", e)
        }
        message => panic!("Not connection ack = {:?}", message),
    };

    (id, this_rx)
}

fn config(id: u8) -> Config {
    Config {
        id,
        dir: PathBuf::from("/tmp/timestone"),
        max_segment_size: 5 * 1024 * 1024,
        max_segment_count: 1024,
        routers: None,
    }
}

 */
