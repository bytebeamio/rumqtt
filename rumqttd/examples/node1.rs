// use rumqttd::{Link, LinkTx, Router, RouterConfig};
// use bytes::BytesMut;
// use mqttbytes::v4::Publish;
// use mqttbytes::QoS;
// use std::thread;
// use std::time::Duration;
//
// fn spawn_publisher(mut router_tx: LinkTx, count: u8) {
//     thread::sleep(Duration::from_secs(1));
//     let mut pkid = 0;
//
//     thread::spawn(move || loop {
//         let mut o = BytesMut::new();
//         for i in 0..count {
//             let topic = "hello/world";
//             let qos = QoS::AtLeastOnce;
//             let payload = vec![1, 2, i];
//             pkid += 1;
//
//             let mut publish = Publish::new(topic, qos, payload);
//             publish.pkid = pkid;
//             publish.write(&mut o).unwrap();
//         }
//
//         router_tx.send_device_data(o).unwrap();
//         thread::sleep(Duration::from_secs(10));
//     });
// }
//
// fn main() {
//     pretty_env_logger::init();
//     let max_segment_size = 100 * 1024;
//     let max_segment_count = 10;
//     let max_connections = 10;
//     let config =
//         RouterConfig { instant_ack: false, max_segment_size, max_segment_count, max_connections };
//
//     let router = Router::new(0, config);
//     let link = router.start_with_cluster("localhost:7070", Vec::<(usize, String)>::new());
//     let (link_tx, mut link_rx) = Link::new("device@0", link).unwrap();
//
//     spawn_publisher(link_tx, 5);
//
//     // Receiver
//     loop {
//         let notification = link_rx.recv().unwrap();
//         println!("Received = {:?}", notification);
//     }
// }

fn main() {}
