use derive_more::From;
use futures_util::future::FutureExt;
use futures_util::select;
use futures_util::stream::StreamExt;
use rumq_core::{MqttRead, MqttWrite};
use tokio::sync::mpsc::{self, channel, Receiver, Sender};
use tokio::time;

use crate::graveyard::Graveyard;
use crate::router::RouterMessage;
use crate::state::{self, MqttState};
use crate::Network;
use crate::ServerSettings;

use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, From)]
pub enum Error {
    State(state::Error),
    Core(rumq_core::Error),
    Timeout(time::Elapsed),
    Mpsc(mpsc::error::SendError<RouterMessage>),
}

pub async fn eventloop(
    config: Arc<ServerSettings>,
    graveyard: Graveyard,
    stream: impl Network,
    mut router_tx: Sender<RouterMessage>,
) -> Result<String, Error> {
    // state of the given connection
    let mut state = MqttState::new();

    // TODO: persistent new connection should get state back from the graveyard and send pending packets
    // along with outstanding publishes that the router received when the client connection is offline
    let mut connection = Connection::new(config, &mut state, stream, router_tx.clone()).await?;
    let id = connection.id.clone();

    if let Err(err) = connection.run().await {
        let id = id.clone();
        error!("Connection error = {:?}", err);

        match err {
            Error::State(state::Error::Disconnect(id)) => {
                router_tx.send(RouterMessage::Disconnect(id)).await?
            }
            _ => router_tx.send(RouterMessage::Death(id)).await?,
        }
    }

    info!("Reaping the connection in graveyard. Id = {}", id);
    graveyard.reap(&id, state);

    Ok(id)
}

struct Connection<'eventloop, S> {
    id:         String,
    keep_alive: Duration,
    state:      &'eventloop mut MqttState,
    stream:     S,
    this_rx:    Receiver<RouterMessage>,
    router_tx:  Sender<RouterMessage>,
}

impl<'eventloop, S: Network> Connection<'eventloop, S> {
    async fn new(
        config: Arc<ServerSettings>,
        state: &'eventloop mut MqttState,
        mut stream: S,
        mut router_tx: Sender<RouterMessage>,
    ) -> Result<Connection<'eventloop, S>, Error> {
        let (this_tx, this_rx) = channel(100);
        let timeout = Duration::from_millis(config.connection_timeout_ms.into());
        let (id, keep_alive, connack) = time::timeout(timeout, async {
            let packet = stream.mqtt_read().await?;
            let o = state.handle_incoming_connect(packet)?;
            Ok::<_, Error>(o)
        })
        .await??;

        // write connack packet
        stream.mqtt_write(&connack).await?;
        // construct connect router message with cliend id and handle to this connection
        let routermessage = RouterMessage::Connect((id.clone(), this_tx));
        router_tx.send(routermessage).await?;
        let connection = Connection { id, keep_alive, state, stream, this_rx, router_tx };
        Ok(connection)
    }

    async fn run(&mut self) -> Result<(), Error> {
        // TODO: Enable and monitor perf. Default buffer size of 8K. Might've to periodically flush?
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
