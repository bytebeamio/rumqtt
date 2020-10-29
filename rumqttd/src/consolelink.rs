use rumqttlog::ConnectionId;
use rumqttlog::{Connection, ConnectionAck, Event, Notification, Receiver, Sender};
use std::sync::Arc;
use warp::Filter;

pub struct ConsoleLink {
    id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    link_rx: Receiver<Notification>,
}

impl ConsoleLink {
    pub fn new(router_tx: Sender<(ConnectionId, Event)>) -> ConsoleLink {
        let (connection, link_rx) = Connection::new_remote("console", true, 10);
        let message = (0, Event::Connect(connection));
        router_tx.send(message).unwrap();

        let (id, _, _) = match link_rx.recv().unwrap() {
            Notification::ConnectionAck(ack) => match ack {
                ConnectionAck::Success((id, session, pending)) => (id, session, pending),
                ConnectionAck::Failure(reason) => unreachable!("{}", reason),
            },
            notification => unreachable!("{:?}", notification),
        };

        ConsoleLink {
            router_tx,
            link_rx,
            id,
        }
    }
}

#[tokio::main(core_threads = 1)]
pub async fn start(console: Arc<ConsoleLink>) {
    let console = console.clone();

    let routes = warp::any().map(move || {
        let message = Event::Metrics(None);
        console.router_tx.send((console.id, message)).unwrap();
        let notification = console.link_rx.recv();
        format!("{:?}", notification)
    });

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}
