#[macro_use]
extern crate log;

use std::io;
use std::time::Duration;
use std::sync::Arc;

use derive_more::From;
use tokio::task;
use tokio::time;
use tokio::time::Elapsed;
use tokio::net::{TcpStream, TcpListener, ToSocketAddrs};
use tokio::io::BufReader;
use async_std::sync::{channel, Sender, Receiver};
use sharded_slab::Slab;
use rumq_core::{Packet, MqttRead, MqttWrite};

mod client;
mod state;
mod router;

#[derive(From, Debug)]
pub enum Error {
    Io(io::Error),
    Mqtt(rumq_core::Error),
    Timeout(Elapsed),
    State(state::Error),
    Disconnected,
}

async fn connection_loop(mut stream: TcpStream, slab: Arc<Slab<Sender<Packet>>>) -> Result<(), Error> {
    let mut mqtt = state::MqttState::new();
    let timeout = Duration::from_millis(100);
    
    time::timeout(timeout, async {
        let packet = stream.mqtt_read().await?;
        let notification_and_reply = mqtt.handle_incoming_connect(packet)?;
        
        if let Some(packet) = notification_and_reply.1 {
            stream.mqtt_write(&packet).await?;
        }
        
        Ok::<_, Error>(())
    }).await??;
    
    let mut reader = BufReader::new(stream); // 2
    while let packet = reader.mqtt_read().await? { // 4
        debug!("Packet = {:?}", packet);
    }
    
    Ok(())
}

pub struct Broker {}

pub async fn accept_loop(addr: &str) -> Result<(), Error> {
    let mut listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();

    let router = Arc::new(Slab::new());

    info!("Waiting for connection");
    loop {
        let (tx, rx) = channel::<Packet>(2);
        let key = router.insert(tx).unwrap();

        let (stream, addr) = listener.accept().await?;
        info!("Accepting from: {}", addr); 
        
        let _handle = task::spawn(connection_loop(stream, router.clone()));
    }

    Ok(())
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
