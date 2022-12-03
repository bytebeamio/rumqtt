use crate::protocol::{
    ConnAck, Filter, LastWill, Packet, Publish, QoS, RetainForwardRule, Subscribe,
};
use crate::router::Ack;
use crate::router::{
    iobufs::{Incoming, Outgoing},
    Connection, Event, Notification, ShadowRequest,
};
use crate::ConnectionId;
use bytes::Bytes;
use flume::{Receiver, RecvError, RecvTimeoutError, SendError, Sender, TrySendError};
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};

use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error("Unexpected router message")]
    NotConnectionAck,
    #[error("ConnAck error {0}")]
    ConnectionAck(String),
    #[error("Channel try send error")]
    TrySend(#[from] TrySendError<(ConnectionId, Event)>),
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Channel recv error")]
    Recv(#[from] RecvError),
    #[error("Channel timeout recv error")]
    RecvTimeout(#[from] RecvTimeoutError),
    #[error("Timeout = {0}")]
    Elapsed(#[from] tokio::time::error::Elapsed),
}

pub struct Link;

impl Link {
    #[allow(clippy::type_complexity)]
    fn prepare(
        tenant_id: Option<String>,
        client_id: &str,
        clean: bool,
        last_will: Option<LastWill>,
        dynamic_filters: bool,
    ) -> (
        Event,
        Arc<Mutex<VecDeque<Packet>>>,
        Arc<Mutex<VecDeque<Notification>>>,
        Receiver<()>,
    ) {
        let connection = Connection::new(
            tenant_id,
            client_id.to_owned(),
            clean,
            last_will,
            dynamic_filters,
        );
        let incoming = Incoming::new(client_id.to_string());
        let (outgoing, link_rx) = Outgoing::new(client_id.to_string());
        let outgoing_data_buffer = outgoing.buffer();
        let incoming_data_buffer = incoming.buffer();

        let event = Event::Connect {
            connection,
            incoming,
            outgoing,
        };

        (event, incoming_data_buffer, outgoing_data_buffer, link_rx)
    }

    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        tenant_id: Option<String>,
        client_id: &str,
        router_tx: Sender<(ConnectionId, Event)>,
        clean: bool,
        last_will: Option<LastWill>,
        dynamic_filters: bool,
    ) -> Result<(LinkTx, LinkRx, Notification), LinkError> {
        // Connect to router
        // Local connections to the router shall have access to all subscriptions

        let (message, i, o, link_rx) =
            Link::prepare(tenant_id, client_id, clean, last_will, dynamic_filters);
        router_tx.send((0, message))?;

        link_rx.recv()?;
        let notification = o.lock().pop_front().unwrap();

        // Right now link identifies failure with dropped rx in router,
        // which is probably ok. We need this here to get id assigned by router
        let id = match notification {
            Notification::DeviceAck(Ack::ConnAck(id, ..)) => id,
            _message => return Err(LinkError::NotConnectionAck),
        };

        let tx = LinkTx::new(id, router_tx.clone(), i);
        let rx = LinkRx::new(id, router_tx, link_rx, o);
        Ok((tx, rx, notification))
    }

    pub async fn init(
        tenant_id: Option<String>,
        client_id: &str,
        router_tx: Sender<(ConnectionId, Event)>,
        clean: bool,
        last_will: Option<LastWill>,
        dynamic_filters: bool,
    ) -> Result<(LinkTx, LinkRx, ConnAck), LinkError> {
        // Connect to router
        // Local connections to the router shall have access to all subscriptions

        let (message, i, o, link_rx) =
            Link::prepare(tenant_id, client_id, clean, last_will, dynamic_filters);
        router_tx.send_async((0, message)).await?;

        link_rx.recv_async().await?;
        let notification = o.lock().pop_front().unwrap();
        // Right now link identifies failure with dropped rx in router,
        // which is probably ok. We need this here to get id assigned by router
        let (id, ack) = match notification {
            Notification::DeviceAck(Ack::ConnAck(id, ack)) => (id, ack),
            _message => return Err(LinkError::NotConnectionAck),
        };

        let tx = LinkTx::new(id, router_tx.clone(), i);
        let rx = LinkRx::new(id, router_tx, link_rx, o);
        Ok((tx, rx, ack))
    }
}

pub struct LinkTx {
    pub(crate) connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    recv_buffer: Arc<Mutex<VecDeque<Packet>>>,
}

impl LinkTx {
    pub fn new(
        connection_id: ConnectionId,
        router_tx: Sender<(ConnectionId, Event)>,
        recv_buffer: Arc<Mutex<VecDeque<Packet>>>,
    ) -> LinkTx {
        LinkTx {
            connection_id,
            router_tx,
            recv_buffer,
        }
    }

    pub fn buffer(&self) -> MutexGuard<RawMutex, VecDeque<Packet>> {
        self.recv_buffer.lock()
    }

    /// Send raw device data
    fn push(&mut self, data: Packet) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .send((self.connection_id, Event::DeviceData))?;

        Ok(len)
    }

    /// Send raw device data
    pub async fn send(&mut self, data: Packet) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .send((self.connection_id, Event::DeviceData))?;

        Ok(len)
    }

    fn try_push(&mut self, data: Packet) -> Result<usize, LinkError> {
        let len = {
            let mut buffer = self.recv_buffer.lock();
            buffer.push_back(data);
            buffer.len()
        };

        self.router_tx
            .try_send((self.connection_id, Event::DeviceData))?;
        Ok(len)
    }

    pub(crate) async fn notify(&mut self) -> Result<(), LinkError> {
        self.router_tx
            .send_async((self.connection_id, Event::DeviceData))
            .await?;

        Ok(())
    }

    /// Sends a MQTT Publish to the router
    pub fn publish<S, V>(&mut self, topic: S, payload: V) -> Result<usize, LinkError>
    where
        S: Into<Bytes>,
        V: Into<Bytes>,
    {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            pkid: 0,
            payload: payload.into(),
        };

        let len = self.push(Packet::Publish(publish, None))?;
        Ok(len)
    }

    /// Sends a MQTT Publish to the router
    pub fn try_publish<S, V>(&mut self, topic: S, payload: V) -> Result<usize, LinkError>
    where
        S: Into<Bytes>,
        V: Into<Bytes>,
    {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            pkid: 0,
            payload: payload.into(),
        };

        let len = self.try_push(Packet::Publish(publish, None))?;
        // TODO: Remote item in buffer after failure and write unittest
        Ok(len)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, filter: S) -> Result<usize, LinkError> {
        let filters = vec![Filter {
            path: filter.into(),
            qos: QoS::AtMostOnce,
            nolocal: false,
            preserve_retain: false,
            retain_forward_rule: RetainForwardRule::Never,
        }];

        let subscribe = Subscribe { pkid: 0, filters };

        let len = self.push(Packet::Subscribe(subscribe, None))?;
        Ok(len)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(&mut self, filter: S) -> Result<usize, LinkError> {
        let filters = vec![Filter {
            path: filter.into(),
            qos: QoS::AtMostOnce,
            nolocal: false,
            preserve_retain: false,
            retain_forward_rule: RetainForwardRule::Never,
        }];

        let subscribe = Subscribe { pkid: 0, filters };

        let len = self.try_push(Packet::Subscribe(subscribe, None))?;
        Ok(len)
    }

    /// Request to get device shadow
    pub fn shadow<S: Into<String>>(&mut self, filter: S) -> Result<(), LinkError> {
        let message = Event::Shadow(ShadowRequest {
            filter: filter.into(),
        });

        self.router_tx.try_send((self.connection_id, message))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LinkRx {
    connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    router_rx: Receiver<()>,
    send_buffer: Arc<Mutex<VecDeque<Notification>>>,
    cache: VecDeque<Notification>,
}

impl LinkRx {
    pub fn new(
        connection_id: ConnectionId,
        router_tx: Sender<(ConnectionId, Event)>,
        router_rx: Receiver<()>,
        outgoing_data_buffer: Arc<Mutex<VecDeque<Notification>>>,
    ) -> LinkRx {
        LinkRx {
            connection_id,
            router_tx,
            router_rx,
            send_buffer: outgoing_data_buffer,
            cache: VecDeque::with_capacity(100),
        }
    }

    pub fn id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn recv(&mut self) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        match self.cache.pop_front() {
            Some(v) => Ok(Some(v)),
            None => {
                // If cache is empty, check for router trigger and get fresh notifications
                self.router_rx.recv()?;
                // Collect 'all' the data in the buffer after a notification.
                // Notification means fresh data which isn't previously collected
                mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
                Ok(self.cache.pop_front())
            }
        }
    }

    pub fn recv_deadline(&mut self, deadline: Instant) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        match self.cache.pop_front() {
            Some(v) => Ok(Some(v)),
            None => {
                // If cache is empty, check for router trigger and get fresh notifications
                self.router_rx.recv_deadline(deadline)?;
                mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
                Ok(self.cache.pop_front())
            }
        }
    }

    pub async fn next(&mut self) -> Result<Option<Notification>, LinkError> {
        // Read from cache first
        // One router_rx trigger signifies a bunch of notifications. So we
        // should always check cache first
        match self.cache.pop_front() {
            Some(v) => Ok(Some(v)),
            None => {
                // If cache is empty, check for router trigger and get fresh notifications
                self.router_rx.recv_async().await?;
                // Collect 'all' the data in the buffer after a notification.
                // Notification means fresh data which isn't previously collected
                mem::swap(&mut *self.send_buffer.lock(), &mut self.cache);
                Ok(self.cache.pop_front())
            }
        }
    }

    pub(crate) async fn exchange(
        &mut self,
        notifications: &mut VecDeque<Notification>,
    ) -> Result<(), LinkError> {
        self.router_rx.recv_async().await?;
        mem::swap(&mut *self.send_buffer.lock(), notifications);
        Ok(())
    }

    pub fn ready(&self) -> Result<(), LinkError> {
        self.router_tx.send((self.connection_id, Event::Ready))?;
        Ok(())
    }

    pub async fn wake(&self) -> Result<(), LinkError> {
        self.router_tx
            .send_async((self.connection_id, Event::Ready))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::LinkTx;
    use flume::bounded;
    use parking_lot::Mutex;
    use std::{collections::VecDeque, sync::Arc, thread};

    #[test]
    fn push_sends_all_data_and_notifications_to_router() {
        let (router_tx, router_rx) = bounded(10);
        let mut buffers = Vec::new();
        const CONNECTIONS: usize = 1000;
        const MESSAGES_PER_CONNECTION: usize = 100;

        for i in 0..CONNECTIONS {
            let buffer = Arc::new(Mutex::new(VecDeque::new()));
            let mut link_tx = LinkTx::new(i, router_tx.clone(), buffer.clone());
            buffers.push(buffer);
            thread::spawn(move || {
                for _ in 0..MESSAGES_PER_CONNECTION {
                    link_tx.publish("hello/world", vec![1, 2, 3]).unwrap();
                }
            });
        }

        // Router should receive notifications from all the connections
        for _ in 0..CONNECTIONS * MESSAGES_PER_CONNECTION {
            let _v = router_rx.recv().unwrap();
        }

        // Every connection has expected number of messages
        for item in buffers.iter().take(CONNECTIONS) {
            assert_eq!(item.lock().len(), MESSAGES_PER_CONNECTION);
        }

        // TODO: Write a similar test to benchmark buffer vs channels
    }
}
