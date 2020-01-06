use derive_more::From;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::{Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{self, channel, Receiver, Sender};
use tokio::time;

use rumq_core::*;

use crate::state::{self, MqttState};
use crate::graveyard::Graveyard;
use crate::ConnectionConfig;

use std::time::Duration;

#[derive(Debug, From)]
pub enum Error {
    State(state::Error),
    Core(rumq_core::Error),
    Timeout(time::Elapsed),
    Mpsc(mpsc::error::SendError<Packet>)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
}

pub async fn eventloop(config: ConnectionConfig, graveyard: Graveyard, mut stream: impl Network, mut router_data_tx: Sender<Packet>) -> Result<(), Error> {
    // state of the given connection
    let mut state = MqttState::new();
    
    // read connect packet with a time to prevent dos attacks
    let timeout = Duration::from_millis(100);
    time::timeout(timeout, async {
        let packet = stream.mqtt_read().await?;
        let connack = state.handle_incoming_connect(packet)?;
        stream.mqtt_write(&connack).await?;
        Ok::<_, Error>(())
    }).await??;
  
    // handle to this connection
    let (this_tx, mut this_rx) = channel(100);

    // eventloop which processes packets and router messages
    loop {
        let (routerpacket, outpacket) = select! {
            // read packets from network and generate network reply and router message
            o = stream.mqtt_read().fuse() => state.handle_incoming_mqtt_packet(o?)?,
            // read packets from router and generate packets to write to network
            o = this_rx.next().fuse() => {
                let packet = state.handle_outgoing_mqtt_packet(o.unwrap())?;
                (None, Some(packet))
            }
        };

        // TODO: Adding a bufreader and writer might improve read and write perf for 
        // small frequent messages. Not sure how to do this on TcpStream to do both
        // read and write with out using `io::split`. `io::split` introduces locks which
        // might affect perf during high load
        // let mut stream = BufReader::new(stream);
        
        // send packet to router
        if let Some(packet) = routerpacket {
            router_data_tx.send(packet).await?;
        }

        // write packet to network
        if let Some(packet) = outpacket {
            stream.mqtt_write(&packet).await?;
        }
    }
}

pub trait Network: AsyncWrite + AsyncRead + Unpin + Send {}
impl<T> Network for T where T: AsyncWrite + AsyncRead + Unpin + Send {}
