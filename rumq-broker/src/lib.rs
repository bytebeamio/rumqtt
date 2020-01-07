#[macro_use]
extern crate log;

use derive_more::From;
use rumq_core::Packet;
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;
use tokio::task;
use tokio::time::{self, Elapsed};

use std::io;
use std::time::Duration;

mod graveyard;
mod connection;
mod router;
mod state;

#[derive(From, Debug)]
pub enum Error {
    Io(io::Error),
    Mqtt(rumq_core::Error),
    Timeout(Elapsed),
    State(state::Error),
    Disconnected,
}


/// Quotas and limitations mirroring google cloud
/// TODO: Review all limitations and audit security aspects
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Max device id length
    device_id_length: usize,
    /// Maximum payload size of telemetry event
    telemetry_payload_size: usize,
    /// Throughput from cloud to device
    cloud_to_device_throughput: usize,
    /// Throughput from device to cloud
    device_to_cloud_throughput: usize,
    /// Minimum delay time between consecutive outgoing packets
    incoming_messages_per_sec: usize,
    /// maximum size of all inflight messages
    inflight_size: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self { 
        ConnectionConfig {
            device_id_length: 256,
            // 100KB per event
            telemetry_payload_size: 100 * 1024,
            // 100 KB/s throughput
            cloud_to_device_throughput: 100 * 1024,
            // 100 KB/s throughput
            device_to_cloud_throughput: 100 * 1024,
            // 10ms constant delay between each event
            incoming_messages_per_sec: 100,
            // 10MB
            inflight_size: 10 * 1024 * 1024,
        } 
    }
}

pub async fn accept_loop(addr: &str) -> Result<(), Error> {
    let connection_config = ConnectionConfig::default();
    let mut listener = TcpListener::bind(addr).await?;
    let (router_tx, router_rx) = channel::<Packet>(10);

    // graveyard to save state of persistent connections
    let graveyard = graveyard::Graveyard::new();
    
    // router to route data between connections. creates an extra copy but
    // might not be a big deal if we prevent clones/send fat pointers and batch
    let graveyard2 = graveyard.clone();
    task::spawn(async move {
        let mut router = router::Router::new(graveyard2, router_rx);
        router.start().await
    });
    
    info!("Waiting for connection");
    // eventloop which accepts connections
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Accepting from: {}", addr);
        task::spawn(connection::eventloop(connection_config.clone(), graveyard.clone(), stream, router_tx.clone()));
        time::delay_for(Duration::from_millis(1)).await;
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn accept_loop_rate_limits_incoming_connections() {}

    #[test]
    fn accept_loop_should_not_allow_more_than_maximum_connections() {}

    #[test]
    fn accept_loop_should_accept_new_connection_when_a_client_disconnects_after_max_connections() {}

    #[test]
    fn client_loop_should_error_if_connect_packet_is_not_received_in_time() {}
}
