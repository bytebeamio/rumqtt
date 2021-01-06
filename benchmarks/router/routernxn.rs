// use argh::FromArgs;
// use rumqttlog::tracker::Tracker;
// use rumqttlog::{Config, Router, RouterInMessage, RouterOutMessage, Sender};
// use mqtt4bytes::*;
// use std::thread;
// use std::time::{Duration, Instant};
// use tokio::{task, select, time};
// use tokio::stream::StreamExt;
// use async_channel::{Receiver, bounded};
// use std::sync::Arc;
//
// pub mod common;
//
// #[derive(Clone, FromArgs)]
// /// Reach new heights.
// struct CommandLine {
//     /// size of payload
//     #[argh(option, short = 'b', default = "100")]
//     payload_size: usize,
//     /// number of messages
//     #[argh(option, short = 'n', default = "20")]
//     message_count: usize,
//     /// number of topics over which data is being sent
//     #[argh(option, short = 't', default = "1")]
//     topic_count: usize,
//     /// maximum segment size
//     #[argh(option, short = 'z', default = "1*1024*1024")]
//     segment_size: usize,
//     /// maximum sweep read size
//     #[argh(option, short = 'y', default = "100*1024")]
//     sweep_size: usize,
//     /// number of subscribers
//     #[argh(option, short = 's', default = "4")]
//     subscriber_count: usize,
//     /// number of publishers
//     #[argh(option, short = 'p', default = "4")]
//     publisher_count: usize,
// }
//
// #[tokio::main(core_threads = 8)]
// async fn main() {
//     pretty_env_logger::init();
//     let commandline: CommandLine = argh::from_env();
//
//     let config = Config {
//         id: 0,
//         dir: Default::default(),
//         max_segment_size: commandline.segment_size,
//         max_segment_count: 10000,
//         max_connections: 100,
//         mesh: None,
//     };
//
//     let (router, tx) = Router::new(Arc::new(config));
//     thread::spawn(move || start_router(router));
//
//     let (network_tx, network_rx) = bounded(100);
//
//     // start connection
//     let router_tx = tx.clone();
//     let cli = commandline.clone();
//     task::spawn(async move {
//         connection(cli, "connection-1", network_rx, router_tx).await;
//     });
//
//     // start network publisher
//     let cli = commandline.clone();
//     task::spawn(async move {
//         let network_tx = network_tx;
//         for i in 0..cli.message_count {
//             let payload = vec![i as u8; cli.payload_size];
//             let mut packet = Publish::new("hello/mqtt", QoS::AtLeastOnce, payload);
//             packet.set_pkid((i % 65000) as u16 + 1);
//             network_tx.send(packet).await.unwrap();
//         }
//     });
//
//     time::delay_for(Duration::from_secs(500)).await;
// }
//
// #[tokio::main(core_threads = 1)]
// async fn start_router(mut router: Router) {
//     router.start().await;
// }
//
// async fn connection(
//     commandline: CommandLine,
//     id: &str,
//     mut network_rx: Receiver<Publish>,
//     router_tx: Sender<(usize, RouterInMessage)>
// ) {
//     let (id, mut this_rx) = common::new_connection(id, 10, &router_tx).await;
//     let mut tracker = Tracker::new(100);
//     tracker.add_subscription("#");
//     let mut got_last_reply = true;
//     let count = 0;
//     let start = Instant::now();
//
//     loop {
//         if got_last_reply {
//             if let Some(message) = tracker.next() {
//                 router_tx.send((id, message)).await.unwrap();
//                 got_last_reply = false;
//             }
//         }
//
//         select! {
//             Some(message) = this_rx.next() => {
//                 match message {
//                     RouterOutMessage::AcksReply(_ack) => continue,
//                     RouterOutMessage::TopicsReply(reply) => {
//                         println!("Received = {:?}", reply);
//                         tracker.track_new_topics(&reply);
//                         got_last_reply = true;
//                     }
//                     RouterOutMessage::DataReply(reply) => {
//                         // println!("Received = {:?}, {:?}, {:?}, {:?}", reply.topic, reply.native_segment, reply.native_offset, reply.native_count);
//                         tracker.update_data_tracker(&reply);
//                         got_last_reply = true;
//
//                         println!("{:?}", count);
//                         // TODO: Fix count
//                         if count == commandline.message_count {
//                             break
//                         }
//                     }
//                     packet => {
//                         panic!("Expecting ack. Received = {:?}", packet);
//                     }
//                 }
//             }
//             Some(publish) = network_rx.next() => {
//                 let data = vec![Incoming::Publish(publish)];
//                 let message = (id, RouterInMessage::Data(data));
//                 router_tx.send(message).await.unwrap();
//             }
//         }
//     }
//
//     let elapsed_ms = start.elapsed().as_millis();
//     let throughput = count as usize / elapsed_ms as usize;
//     let throughput = throughput * 1000;
//     println!("Id = {}, Messages = {}, Payload (bytes) = {}, Throughput = {} messages/s",
//              id,
//              count,
//              commandline.payload_size,
//              throughput,
//     );
// }
//
//

fn main() {
    println!("Hello world!!");
}
