use pprof::ProfilerGuard;
use prost::Message;
use rumqttlog::router::ConnectionAck;
use rumqttlog::{Connection, Event, Notification, Receiver, Sender};
use std::fs::File;
use std::io::Write;

#[allow(unused)]
pub fn profile(name: &str, guard: ProfilerGuard) {
    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}

#[allow(unused)]
pub async fn new_connection(
    id: &str,
    cap: usize,
    router_tx: &Sender<(usize, Event)>,
) -> (usize, Receiver<Notification>) {
    let (connection, this_rx) = Connection::new_remote(id, true, cap);

    // send a connection request with a dummy id
    let message = (0, Event::Connect(connection));
    router_tx.send(message).unwrap();

    // wait for ack from router
    let (id, ..) = match this_rx.recv().unwrap() {
        Notification::ConnectionAck(ConnectionAck::Success(id)) => id,
        Notification::ConnectionAck(ConnectionAck::Failure(e)) => {
            panic!("Connection failed {:?}", e)
        }
        message => panic!("Not connection ack = {:?}", message),
    };

    (id, this_rx)
}
