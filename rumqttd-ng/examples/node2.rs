// use rumqttd::{Link, Broker, Config, RouterConfig};
//
// fn main() {
//     pretty_env_logger::init();
//
//     let router = Router::new(1, config);
//     let link = router.start_with_cluster("localhost:7071", vec![(0, "localhost:7070")]);
//     let (_link_tx, mut link_rx) = Link::new("local@1", link).unwrap();
//
//     // Receiver
//     loop {
//         let notification = link_rx.recv().unwrap();
//         println!("Received = {:?}", notification);
//     }
// }

fn main() {}
