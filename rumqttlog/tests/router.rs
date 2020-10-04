use rumqttlog::router::Acks;
use rumqttlog::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn new_connection_data_notifies_interested_connections() {
    pretty_env_logger::init();
    let connections = Connections::new();
    let (connection_1_id, _connection_1_rx) = connections.connection("1", 2);
    let (connection_2_id, connection_2_rx) = connections.connection("2", 2);

    connections.subscribe(connection_2_id, "hello/+/world", 1);
    let acks = wait_for_acks(&connection_2_rx).unwrap();
    assert_eq!(acks.len(), 1);

    // Write data. 9 messages, 4 topics. Connection"s capacity is only 2 topics.
    connections.data(connection_1_id, "hello/1/world", vec![1, 2, 3], 1);
    connections.data(connection_1_id, "hello/1/world", vec![4, 5, 6], 2);
    connections.data(connection_1_id, "hello/1/world", vec![10, 11, 12], 3);
    connections.data(connection_1_id, "hello/2/world", vec![13, 14, 15], 4);
    connections.data(connection_1_id, "hello/2/world", vec![16, 17, 18], 5);
    connections.data(connection_1_id, "hello/2/world", vec![19, 20, 21], 6);
    connections.data(connection_1_id, "hello/3/world", vec![22, 23, 24], 7);
    connections.data(connection_1_id, "hello/3/world", vec![25, 26, 27], 8);
    connections.data(connection_1_id, "hello/3/world", vec![28, 29, 30], 9);
    connections.data(connection_1_id, "hello/4/world", vec![31, 32, 33], 10);
    connections.data(connection_1_id, "hello/4/world", vec![34, 35, 36], 11);
    connections.data(connection_1_id, "hello/4/world", vec![37, 38, 39], 12);

    connections.ready(connection_2_id);

    let data = wait_for_data(&connection_2_rx).unwrap();
    assert_eq!(data.payload.len(), 3);
    assert_eq!(data.payload[0].as_ref(), &[1, 2, 3]);
    assert_eq!(data.payload[1].as_ref(), &[4, 5, 6]);
    assert_eq!(data.payload[2].as_ref(), &[10, 11, 12]);

    let data = wait_for_data(&connection_2_rx).unwrap();
    assert_eq!(data.payload.len(), 3);
    assert_eq!(data.payload[0].as_ref(), &[13, 14, 15]);
    assert_eq!(data.payload[1].as_ref(), &[16, 17, 18]);
    assert_eq!(data.payload[2].as_ref(), &[19, 20, 21]);
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
        Ok(v) => {
            println!("Error = {:?}", v);
            None
        }
        Err(e) => {
            println!("Error = {:?}", e);
            None
        }
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
            let _ = router.start();
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
