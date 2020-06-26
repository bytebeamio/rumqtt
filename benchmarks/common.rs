use pprof::ProfilerGuard;
use prost::Message;
use std::fs::File;
use std::io::Write;
use rumqttlog::{Sender, RouterInMessage, Receiver, RouterOutMessage, Connection};
use rumqttlog::router::ConnectionAck;

#[allow(unused)]
pub fn profile(name: &str, guard: ProfilerGuard) {
    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode( & mut content).unwrap();
        file.write_all( & content).unwrap();
    };
}

#[allow(unused)]
pub async fn new_connection(
    id: &str,
    cap: usize,
    router_tx: &Sender<(usize, RouterInMessage)>
) -> (usize, Receiver<RouterOutMessage>) {
    let (connection, this_rx) = Connection::new(id, cap);

    // send a connection request with a dummy id
    let message = (0, RouterInMessage::Connect(connection));
    router_tx.send(message).await.unwrap();

    // wait for ack from router
    let id = match this_rx.recv().await.unwrap() {
        RouterOutMessage::ConnectionAck(ConnectionAck::Success(id)) => id,
        RouterOutMessage::ConnectionAck(ConnectionAck::Failure(e)) => {
            panic!("Connection failed {:?}", e)
        }
        message => panic!("Not connection ack = {:?}", message),
    };

    (id, this_rx)
}
