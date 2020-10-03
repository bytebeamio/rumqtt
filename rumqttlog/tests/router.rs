use rumqttlog::router::Acks;
use rumqttlog::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn new_connection_data_notifies_interested_connections() {
    pretty_env_logger::init();
    let connections = Connections::new();
    let (connection_1_id, _connection_1_rx) = connections.connection("1", 5);
    let (connection_2_id, connection_2_rx) = connections.connection("2", 5);

    connections.subscribe(connection_2_id, "hello/world", 1);

    // write data from a native connection and read from connection
    connections.data(connection_1_id, "hello/world", vec![1, 2, 3], 1);
    connections.data(connection_1_id, "hello/world", vec![4, 5, 6], 2);
    connections.data(connection_1_id, "hello/world", vec![7, 8, 9], 3);

    connections.ready(connection_2_id);
    let reply = wait_for_data(&connection_2_rx).unwrap();
    assert_eq!(reply.payload.len(), 3);
    assert_eq!(reply.payload[0].as_ref(), &[1, 2, 3]);
    assert_eq!(reply.payload[1].as_ref(), &[4, 5, 6]);
    assert_eq!(reply.payload[2].as_ref(), &[7, 8, 9]);
}

fn wait_for_data(rx: &Receiver<Notification>) -> Option<Data> {
    thread::sleep(Duration::from_secs(1));

    match rx.try_recv() {
        Ok(Notification::Data(reply)) => Some(reply),
        Ok(v) => panic!("{:?}", v),
        Err(e) => panic!("{:?}", e),
    }
}

fn wait_for_acks(rx: &Receiver<Notification>) -> Option<Acks> {
    thread::sleep(Duration::from_secs(1));

    match rx.try_recv() {
        Ok(Notification::Acks(reply)) => Some(reply),
        Ok(v) => panic!("{:?}", v),
        Err(e) => panic!("{:?}", e),
    }
}

// Broker is used to test router
pub(crate) struct Connections {
    router_tx: Sender<(ConnectionId, Event)>,
}

impl Connections {
    pub fn new() -> Connections {
        let mut config = Config::default();
        config.id = 0;

        let (router, router_tx) = Router::new(Arc::new(config));
        thread::spawn(move || {
            let mut router = router;
            router.start();
        });

        Connections { router_tx }
    }

    pub fn connection(&self, id: &str, cap: usize) -> (ConnectionId, Receiver<Notification>) {
        let (connection, link_rx) = rumqttlog::Connection::new_remote(id, cap);
        let message = Event::Connect(connection);
        self.router_tx.send((0, message)).unwrap();

        let connection_id = match link_rx.recv().unwrap() {
            Notification::ConnectionAck(ConnectionAck::Success(id)) => id,
            o => panic!("Unexpected connection ack = {:?}", o),
        };

        (connection_id, link_rx)
    }

    pub fn ready(&self, id: usize) {
        let message = (id, Event::Ready);
        self.router_tx.try_send(message).unwrap();
    }

    pub fn data(&self, id: usize, topic: &str, payload: Vec<u8>, pkid: u16) {
        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.pkid = pkid;

        let message = Event::Data(vec![Packet::Publish(publish)]);
        let message = (id, message);
        self.router_tx.try_send(message).unwrap();
    }

    pub fn subscribe(&self, id: usize, filter: &str, pkid: u16) {
        let mut subscribe = Subscribe::new(filter, QoS::AtLeastOnce);
        subscribe.pkid = pkid;

        let message = Event::Data(vec![Packet::Subscribe(subscribe)]);
        let message = (id, message);
        self.router_tx.try_send(message).unwrap();
    }
}
