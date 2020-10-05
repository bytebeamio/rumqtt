use rumqttlog::router::Acks;
use rumqttlog::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn acks_are_retuned_as_expected_to_the_connection() {
    let connections = Connections::new();
    let (connection_1_id, connection_1_rx) = connections.connection("1", 3);

    // Send data one by one
    for i in 0..1000 {
        connections.data(connection_1_id, "hello/1/world", vec![1, 2, 3], i);
    }

    let mut count = 0;
    loop {
        count += wait_for_acks(&connection_1_rx).unwrap().len();

        if count == 1000 {
            break;
        }
    }

    // Send data in bulk
    connections.datav(connection_1_id, "hello/1/world", vec![1, 2, 3], 1000);
    let mut count = 0;
    loop {
        count += wait_for_acks(&connection_1_rx).unwrap().len();

        if count == 1000 {
            break;
        }
    }
}

#[test]
fn new_connection_data_notifies_interested_connections() {
    pretty_env_logger::init();
    let connections = Connections::new();
    let (connection_1_id, _connection_1_rx) = connections.connection("1", 2);
    let (connection_2_id, connection_2_rx) = connections.connection("2", 2);

    connections.subscribe(connection_2_id, "hello/+/world", 1);
    let acks = wait_for_acks(&connection_2_rx).unwrap();
    assert_eq!(acks.len(), 1);

    // Write data. 4 messages, 2 topics. Connection's capacity is 2 items.
    connections.data(connection_1_id, "hello/1/world", vec![1, 2, 3], 1);
    connections.data(connection_1_id, "hello/1/world", vec![4, 5, 6], 2);
    connections.data(connection_1_id, "hello/2/world", vec![13, 14, 15], 4);
    connections.data(connection_1_id, "hello/2/world", vec![16, 17, 18], 5);

    // Pending requests were done before. Readiness has
    // to be manually triggered

    let data = wait_for_data(&connection_2_rx).unwrap();
    assert_eq!(data.payload.len(), 1);
    assert_eq!(data.payload[0].as_ref(), &[1, 2, 3]);

    let data = wait_for_data(&connection_2_rx).unwrap();
    assert_eq!(data.payload.len(), 3);
    assert_eq!(data.payload[0].as_ref(), &[4, 5, 6]);
    assert_eq!(data.payload[1].as_ref(), &[13, 14, 15]);
    assert_eq!(data.payload[2].as_ref(), &[16, 17, 18]);
}

fn wait_for_data(rx: &Receiver<Notification>) -> Option<Data> {
    thread::sleep(Duration::from_secs(1));

    match rx.try_recv() {
        Ok(Notification::Data(reply)) => Some(reply),
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

    pub fn datav(&self, id: usize, topic: &str, payload: Vec<u8>, count: u16) {
        let mut packets = Vec::new();
        for i in 0..count {
            let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload.clone());
            publish.pkid = i;
            packets.push(Packet::Publish(publish));
        }

        let message = Event::Data(packets);
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
