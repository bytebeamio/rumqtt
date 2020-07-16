use crate::router::RouterInMessage;
use crate::{Config, MeshConfig, IO};

mod link;

use async_channel::{bounded, Sender};
use link::Replicator;
use rumqttc::{ConnAck, Connect, ConnectReturnCode, Packet};
use std::collections::HashMap;
use std::io;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;
use tokio::time::{self, Elapsed};
use tokio::{select, task};

#[derive(thiserror::Error, Debug)]
#[error("...")]
pub enum Error {
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    StreamDone,
    ConnectionHandover,
    WrongPacket(Packet),
}

type ConnectionId = usize;

/// Each router maintains a task to communicate with every other broker. Which router initiates the
/// connection is implemented using handehake combination by sequentially following routers in config
pub struct Mesh {
    /// Config which holds details of all the routers for distributed mesh
    config: Config,
    /// Router handle to pass to links
    router_tx: Sender<(ConnectionId, RouterInMessage)>,
    /// Handles to all the links
    links: HashMap<u8, LinkHandle<TcpStream>>,
}

impl Mesh {
    pub(crate) fn new(config: Config, router_tx: Sender<(ConnectionId, RouterInMessage)>) -> Mesh {
        Mesh {
            config,
            router_tx,
            links: HashMap::new(),
        }
    }

    /// This starts the replication thread. Each new connection (incoming or outgoing) is informed
    /// to the router along with the connection handle. Each router connection task can now pull
    /// data it required from the router. Router will put data retrieved from the commitlog into
    /// correct task's handle
    #[tokio::main(core_threads = 1)]
    pub(crate) async fn start(&mut self) {
        let (head, this, tail) = self.extract_servers();

        // start outgoing (client) links and then incoming (server) links
        self.start_replicators(tail, true).await;
        self.start_replicators(head, false).await;

        let addr = format!("{}:{}", this.host, this.port);
        let mut listener = TcpListener::bind(&addr).await.unwrap();
        info!("Listening on address: {}", addr);

        // start the supervision
        loop {
            let (stream, addr) = listener.accept().await.unwrap();
            debug!("Received a tcp connection from {}", addr);
            // let mut framed = Framed::new(stream, MqttCodec::new(10 * 1024));
            // let id = match await_connect(&mut framed).await {
            //     Ok(id) => id,
            //     Err(e) => {
            //         error!("Failed to await connect. Error = {:?}", e);
            //         continue;
            //     }
            // };

            // let handle = self.links.get_mut(&id).unwrap();
            // if let Err(_e) = handle.connections_tx.send(framed).await {
            //     error!("Failed to send the connection to link");
            // }
        }
    }

    /// launch client replicators. We'll connect later
    async fn start_replicators(&mut self, config: Vec<MeshConfig>, is_client: bool) {
        for server in config.iter() {
            let (connections_tx, connections_rx) = bounded(1);
            let router_tx = self.router_tx.clone();
            let addr = format!("{}:{}", server.host, server.port);
            let id = server.id;
            let link_handle = LinkHandle::new(server.id, addr, connections_tx);
            self.links.insert(id, link_handle);

            task::spawn(async move {
                let replicator = Replicator::new(id, router_tx, connections_rx, is_client).await;
                replicator.start().await;
            });
        }
    }

    /// Extract routers from the config. Returns
    /// - Incoming connections that this router is expecting
    /// - Config of this router
    /// - Outgoing connections that this router should make
    fn extract_servers(&self) -> (Vec<MeshConfig>, MeshConfig, Vec<MeshConfig>) {
        let id = self.config.id.clone();
        let mut routers = self.config.routers.clone().unwrap();
        let position = routers.iter().position(|v| v.id == id);
        let position = position.unwrap();

        let tail = routers.split_off(position + 1);
        let (this, head) = routers.split_last().unwrap().clone();

        (head.into(), this.clone(), tail)
    }
}

/// Await mqtt connect packet for incoming connections from a router
async fn await_connect(framed: &mut Box<dyn IO>) -> Result<u8, Error> {
    // let id = time::timeout(Duration::from_secs(5), async {
    //     // wait for mesh connect packet with id
    //     let packet = match framed.next().await {
    //         Some(packet) => packet,
    //         None => return Err(Error::StreamDone),
    //     };

        // let connect = match packet? {
        //     Packet::Connect(connect) => connect,
        //     packet => return Err(Error::WrongPacket(packet)),
        // };

    //     let id: u8 = connect.client_id.parse().unwrap();
    //     let connack = ConnAck::new(ConnectReturnCode::Accepted, false);
    //     framed.send(Packet::ConnAck(connack)).await?;
    //     Ok::<_, Error>(id)
    // })
    // .await??;

    // Ok(id)
    todo!()
}

pub struct LinkHandle<S> {
    pub id: u8,
    pub addr: String,
    pub connections_tx: Sender<S>,
}

impl<S: IO> LinkHandle<S> {
    pub fn new(
        id: u8,
        addr: String,
        connections_tx: Sender<S>,
    ) -> LinkHandle<S> {
        LinkHandle {
            id,
            addr,
            connections_tx,
        }
    }

    pub async fn connect(
        &mut self,
        this_id: u8,
        mut framed: S,
    ) -> Result<(), Error> {
        // let connect = Connect::new(this_id.to_string());
        // framed.send(Packet::Connect(connect)).await?;
        // let packet = match framed.next().await {
        //     Some(packet) => packet,
        //     None => return Err(Error::StreamDone),
        // };

        // match packet? {
        //     Packet::ConnAck(_ack) => (),
        //     packet => return Err(Error::WrongPacket(packet)),
        // };

        // if let Err(_) = self.connections_tx.send(framed).await {
        //     return Err(Error::ConnectionHandover);
        // }

        // Ok(())
        todo!()
    }
}

impl<H> Clone for LinkHandle<H> {
    fn clone(&self) -> Self {
        LinkHandle {
            id: self.id,
            addr: self.addr.to_string(),
            connections_tx: self.connections_tx.clone(),
        }
    }
}
