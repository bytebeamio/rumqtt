use rumqttlog::ConnectionId;
use rumqttlog::{Connection, ConnectionAck, Event, Notification, Receiver, Sender};
use std::thread;
use std::time::Duration;

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

    pub fn start(&mut self) {
        loop {
            let message = Event::Metrics(None);
            self.router_tx.send((self.id, message)).unwrap();
            let notification = self.link_rx.recv();
            println!("{:?}", notification);
            thread::sleep(Duration::from_secs(1));
        }
    }
}
