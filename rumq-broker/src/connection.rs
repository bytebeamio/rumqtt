use derive_more::From;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::StreamExt;
use tokio::sync::mpsc::{self, channel, Receiver, Sender};
use tokio::time;
use rumq_core::{MqttRead, MqttWrite};

use crate::graveyard::Graveyard;
use crate::state::{self, MqttState};
use crate::router::RouterMessage;
use crate::ServerSettings;
use crate::Network;

use std::time::Duration;
use std::sync::Arc;

#[derive(Debug, From)]
pub enum Error {
    State(state::Error),
    Core(rumq_core::Error),
    Timeout(time::Elapsed),
    Mpsc(mpsc::error::SendError<RouterMessage>),
}

pub async fn eventloop(config: Arc<ServerSettings>, graveyard: Graveyard, stream: impl Network, router_tx: Sender<RouterMessage>) {
    // state of the given connection
    let mut state = MqttState::new();

    let mut connection = match Connection::new(config, &mut state, stream, router_tx).await {
        Ok(id) => id,
        Err(e) => {
            error!("Connect packet error = {:?}", e);
            return;
        }
    };

    if let Err(e) = connection.run().await {
        error!("Connection error = {:?}", e);
    }

    info!("Reaping the connection in graveyard");
    graveyard.reap(&connection.id, state);
}

struct Connection<'eventloop, S> {
    id: String,
    keep_alive: Duration,
    state:     &'eventloop mut MqttState,
    stream:    S,
    this_rx:   Receiver<RouterMessage>,
    router_tx: Sender<RouterMessage>,
}

impl<'eventloop, S: Network> Connection<'eventloop, S> {
    async fn new(config: Arc<ServerSettings>, state: &'eventloop mut MqttState, mut stream: S, mut router_tx: Sender<RouterMessage>) -> Result<Connection<'eventloop, S>, Error> {
        let (this_tx, this_rx) = channel(100);
        
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let (id, keep_alive, connack) = time::timeout(timeout, async {
            let packet = stream.mqtt_read().await?;
            let o = state.handle_incoming_connect(packet)?;
            Ok::<_, Error>(o)
        }).await??;

        // write connack packet
        stream.mqtt_write(&connack).await?;
        
        // construct connect router message with cliend id and handle to this connection 
        let routermessage = RouterMessage::Connect((id.clone(), this_tx));
        router_tx.send(routermessage).await?;
        let connection = Connection { id, keep_alive, state, stream, this_rx, router_tx };
        
        Ok(connection)
    }


    async fn run(&mut self) -> Result<(), Error> {
        // TODO: Enable to and monitor perf. Default buffer size of 8K. Might've to periodically 
        // flush?
        // Q. What happens when socket receives only 4K of data and there is no new data? 
        // Will the userspace not receive this data indefinitely. I don't see any timeouts?
        // Carl says calls to read only issue one syscall. read_exact will "wait for that 
        // amount to be read
        // Verify BufReader code
        // https://docs.rs/tokio/0.2.6/src/tokio/io/util/buf_reader.rs.html#117
        // let mut stream = BufStream::new(stream);
        
        let id = &self.id;
        
        // eventloop which processes packets and router messages
        loop {
            let stream = &mut self.stream;
            // TODO: Use Delay::reset to not construct this timeout future everytime
            let packet = time::timeout(self.keep_alive, async {
                let packet = stream.mqtt_read().await?;
                Ok::<_, Error>(packet)
            });
            
            let (routerpacket, outpacket) = select! {
                // read packets from network and generate network reply and router message
                o = packet.fuse() => self.state.handle_incoming_mqtt_packet(id, o??)?,
                // read packets from router and generate packets to write to network
                o = self.this_rx.next().fuse() => {
                    let packet = self.state.handle_outgoing_mqtt_packet(o.unwrap())?;
                    (None, Some(packet))
                }
            };


            // send packet to router
            if let Some(packet) = routerpacket {
                self.router_tx.send(packet).await?;
            }

            // write packet to network
            if let Some(packet) = outpacket {
                self.stream.mqtt_write(&packet).await?;
            }
        }
    }
}

