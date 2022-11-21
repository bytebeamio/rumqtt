use crate::link::local::{LinkError, LinkRx, LinkTx};
use crate::local::Link;
use crate::router::{Event, Notification};
use crate::{ConnectionId, ConnectionSettings, Filter};
use bytes::Bytes;
use flume::{RecvError, SendError, Sender, TrySendError};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::error::Elapsed;
use tokio::{select, time};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, tungstenite, WebSocketStream};
use tracing::{debug, error, warn};
use tungstenite::protocol::frame::coding::CloseCode;

use super::network::N;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Valid client id required in topic")]
    InvalidClientId,
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Link error = {0}")]
    Link(#[from] LinkError),
    #[error("Websocket error = {0}")]
    Ws(#[from] tungstenite::Error),
    #[error("Json error = {0}")]
    Json(#[from] serde_json::Error),
    #[error("Shadow filter not set properly")]
    InvalidFilter,
}

pub struct ShadowLink {
    pub(crate) client_id: String,
    pub(crate) connection_id: usize,
    network: Network,
    link_tx: LinkTx,
    link_rx: LinkRx,
    subscriptions: HashSet<Filter>,
}

impl ShadowLink {
    pub async fn new(
        config: Arc<ConnectionSettings>,
        router_tx: Sender<(ConnectionId, Event)>,
        stream: Box<dyn N>,
    ) -> Result<ShadowLink, Error> {
        // Connect to router
        let mut network = Network::new(stream).await?;

        // Wait for MQTT connect packet and error out if it's not received in time to prevent
        // DOS attacks by filling total connections that the server can handle with idle open
        // connections which results in server rejecting new connections
        let connect = network.read_connect(config.connection_timeout_ms).await?;
        let subscriptions = HashSet::new();
        let client_id = connect.client_id.clone();

        let (link_tx, link_rx, _ack) = Link::new(
            None,
            &client_id,
            router_tx,
            true,
            None,
            config.dynamic_filters,
        )?;
        let connection_id = link_rx.id();

        // Send connection acknowledgement back to the client
        network.connack().await?;
        Ok(ShadowLink {
            client_id,
            connection_id,
            network,
            link_tx,
            link_rx,
            subscriptions,
        })
    }

    #[tracing::instrument(skip_all, fields(client_id=self.client_id))]
    pub async fn start(&mut self) -> Result<(), Error> {
        let mut interval = time::interval(Duration::from_secs(5));
        let mut ping = time::interval(Duration::from_secs(10));
        let mut pong = true;

        // Note:
        // Shouldn't result in bounded queue deadlocks because of blocking n/w send
        loop {
            select! {
                o = self.network.read() => {
                    let message = o?;
                    debug!(size = message.len(), "read"  );
                    match message {
                        Message::Text(m) => {
                            self.extract_message(&m).await?;

                        }
                        Message::Close(..) => {
                            let error = "Connection closed".to_string();
                            let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                            return Err(Error::Io(error));
                        }
                        Message::Pong(_) => {
                            pong = true;
                        }
                        packet => {
                            let error = format!("Expecting connect text. Received = {:?}", packet);
                            let error = io::Error::new(io::ErrorKind::InvalidData, error);
                            return Err(Error::Io(error));
                        }
                    };
                }
                // Receive from router when previous when state isn't in collision
                // due to previously received data request
                notification = self.link_rx.next() => {
                    let message = match notification? {
                        Some(v) => v,
                        None => continue,
                    };

                    let message = match message {
                        // TODO: Differentiate pushes and pulls in router
                        Notification::Forward(_forward) => {
                            continue
                        },
                        Notification::DeviceAck(ack) => {
                            warn!(?ack, "Ignoring acks for shadow.");
                            continue;
                        }
                        Notification::Shadow(shadow) => {
                            let topic = std::str::from_utf8(&shadow.topic).unwrap().to_owned();
                            let publish = Outgoing::Publish { topic, data: shadow.payload };
                            Message::Text(serde_json::to_string(&publish)?)
                        }
                        v => unreachable!("Expecting only data or device acks. Received = {:?}", v)
                    };

                    let size = message.len();
                    self.network.write(message).await?;
                    debug!(size,  "write");
                }
                _ = interval.tick() => {
                    for filter in self.subscriptions.iter() {
                        self.link_tx.shadow(filter)?;
                    }
                }
                _ = ping.tick() => {
                    if !pong {
                        error!("no-pong");
                        break
                    }

                    let message = Message::Ping(vec![1, 2, 3]);
                    self.network.write(message).await?;
                    pong = false;
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(client_id=self.client_id))]
    async fn extract_message(&mut self, message: &str) -> Result<(), Error> {
        match serde_json::from_str(message)? {
            Incoming::Shadow { filter } => match validate_shadow(&self.client_id, &filter) {
                Ok(_) => {
                    self.subscriptions.insert(filter.clone());
                    self.link_tx.try_subscribe(filter)?;
                }
                Err(e) => {
                    error!(?e, "validation error");
                    self.network.write(close(&e)).await?;
                    return Err(e);
                }
            },
            Incoming::Ping { .. } => {
                let message = Message::Text(serde_json::to_string(&Outgoing::Pong { pong: true })?);
                self.network.write(message).await?;
            }
            Incoming::Publish { topic, data } => {
                let payload = serde_json::to_vec(&data)?;
                self.link_tx.try_publish(topic, payload)?;
            }
        }

        Ok(())
    }
}

/// Validates that the fields `tenant_id` and `device_id` are the same as that of device connected
/// for a topic filter of format "/tenant/tenant_id/device/device_id/..."
/// Note: Tenant not checked, but could be, in the future
fn validate_shadow(client_id: &String, filter: &str) -> Result<(), Error> {
    let tokens: Vec<&str> = filter.split('/').collect();
    let id = tokens.get(4).ok_or(Error::InvalidFilter)?.to_owned();

    match id == client_id {
        true => Ok(()),
        false => Err(Error::InvalidClientId),
    }
}

fn close(e: &Error) -> Message {
    let msg = match e {
        Error::InvalidClientId => "Provided client id is not valid",
        Error::InvalidFilter => "Shadow filter not set properly",
        _ => "ERROR",
    };
    Message::Close(Some(CloseFrame {
        code: CloseCode::Unsupported,
        reason: Cow::Borrowed(msg),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Connect {
    client_id: String,
    tenant_id: Option<String>,
    body: Option<Jwt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Headers {
    authorization: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Jwt {
    headers: Headers,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type")]
pub enum Outgoing {
    #[serde(alias = "connack")]
    ConnAck { status: bool },
    #[serde(alias = "publish")]
    Publish { topic: String, data: Bytes },
    #[serde(alias = "pong")]
    Pong { pong: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type")]
pub enum Incoming {
    #[serde(alias = "shadow")]
    Shadow { filter: String },
    #[serde(alias = "ping")]
    Ping { ping: bool },
    #[serde(alias = "publish")]
    Publish { topic: String, data: Vec<Value> },
}

/// Network transforms packets <-> frames efficiently. It takes
/// advantage of pre-allocation, buffering and vectorization when
/// appropriate to achieve performance
pub struct Network {
    /// Socket for IO
    socket: WebSocketStream<Box<dyn N>>,
}

impl Network {
    pub async fn new(socket: Box<dyn N>) -> Result<Network, Error> {
        let ws_stream = accept_async(socket).await?;
        Ok(Network { socket: ws_stream })
    }

    pub async fn read_connect(&mut self, t: u16) -> Result<Connect, Error> {
        let message = time::timeout(Duration::from_millis(t.into()), async {
            let message = match self.socket.next().await {
                Some(v) => v?,
                None => {
                    let error = "Connection closed".to_string();
                    let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                    return Err(Error::Io(error));
                }
            };

            Ok::<_, Error>(message)
        })
        .await??;

        let connect: Connect = match message {
            Message::Text(m) => serde_json::from_str(&m)?,
            Message::Close(..) => {
                let error = "Connection closed".to_string();
                let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                return Err(Error::Io(error));
            }
            packet => {
                let error = format!("Expecting connect text. Received = {:?}", packet);
                let error = io::Error::new(io::ErrorKind::InvalidData, error);
                return Err(Error::Io(error));
            }
        };

        if connect.client_id.is_empty() {
            return Err(Error::InvalidClientId);
        }

        Ok(connect)
    }

    pub async fn read(&mut self) -> Result<Message, Error> {
        let message = match self.socket.next().await {
            Some(v) => v?,
            None => {
                let error = "Connection closed".to_string();
                let error = io::Error::new(io::ErrorKind::ConnectionAborted, error);
                return Err(Error::Io(error));
            }
        };

        Ok(message)
    }

    pub async fn connack(&mut self) -> Result<(), Error> {
        let ack = Outgoing::ConnAck { status: true };
        let message = Message::Text(serde_json::to_string(&ack)?);
        self.socket.send(message).await?;
        Ok(())
    }

    pub async fn write(&mut self, message: Message) -> Result<(), Error> {
        self.socket.send(message).await?;
        Ok(())
    }
}
