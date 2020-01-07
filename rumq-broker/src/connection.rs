use derive_more::From;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{self, channel, Receiver, Sender};
use tokio::time;
use rumq_core::{MqttRead, MqttWrite};

use crate::graveyard::Graveyard;
use crate::state::{self, MqttState};
use crate::router::RouterMessage;
use crate::ConnectionConfig;

use std::time::Duration;

#[derive(Debug, From)]
pub enum Error {
    State(state::Error),
    Core(rumq_core::Error),
    Timeout(time::Elapsed),
    Mpsc(mpsc::error::SendError<RouterMessage>),
}

pub async fn eventloop(config: ConnectionConfig, graveyard: Graveyard, stream: impl Network, router_tx: Sender<RouterMessage>) {
    // state of the given connection
    let mut state = MqttState::new();

    let (mut connection, connection_tx) = Connection::new(&mut state, stream, router_tx);
    let id = match connection.await_connect(connection_tx).await {
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
    graveyard.reap(&id, state);
}

struct Connection<'eventloop, S> {
    id: String,
    state:     &'eventloop mut MqttState,
    stream:    S,
    this_rx:   Receiver<RouterMessage>,
    router_tx: Sender<RouterMessage>,
}

impl<'eventloop, S: Network> Connection<'eventloop, S> {
    fn new(state: &'eventloop mut MqttState, stream: S, router_tx: Sender<RouterMessage>) -> (Connection<'eventloop, S>, Sender<RouterMessage>) {
        let (this_tx, this_rx) = channel(100);
        let connection = Connection { id: "uninit".to_owned(), state, stream, this_rx, router_tx };
        
        (connection, this_tx)
    }

    async fn await_connect(&mut self, this_tx: Sender<RouterMessage>) -> Result<String, Error> {
        // read mqtt connect packet with a timeout to prevent dos attacks
        let timeout = Duration::from_millis(100);
        let id = time::timeout(timeout, async {
            let packet = self.stream.mqtt_read().await?;
            let (id, connack) = self.state.handle_incoming_connect(packet)?;

            // write connack packet
            self.stream.mqtt_write(&connack).await?;
            Ok::<_, Error>(id)
        })
        .await??;

        self.id = id.clone();
        // construct connect router message with cliend id and handle to this connection 
        let routermessage = RouterMessage::Connect((id.clone(), this_tx));
        self.router_tx.send(routermessage).await?;
        Ok(id)
    }

    async fn run(&mut self) -> Result<(), Error> {
        let id = &self.id;
        // eventloop which processes packets and router messages
        loop {
            let (routerpacket, outpacket) = select! {
                // read packets from network and generate network reply and router message
                o = self.stream.mqtt_read().fuse() => self.state.handle_incoming_mqtt_packet(id, o?)?,
                // read packets from router and generate packets to write to network
                o = self.this_rx.next().fuse() => {
                    let packet = self.state.handle_outgoing_mqtt_packet(o.unwrap())?;
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
                self.router_tx.send(packet).await?;
            }

            // write packet to network
            if let Some(packet) = outpacket {
                self.stream.mqtt_write(&packet).await?;
            }
        }
    }
}

pub trait Network: AsyncWrite + AsyncRead + Unpin + Send {}
impl<T> Network for T where T: AsyncWrite + AsyncRead + Unpin + Send {}
