//! This module offers a high level synchronous abstraction to async eventloop.
//! Uses channels internally to get `Requests` and send `Notifications`

use crate::{EventLoop, MqttOptions, Incoming, Request, EventLoopError, Outgoing};

use std::time::Duration;
use tokio::{time, select, runtime};
use rumq_core::mqtt4::{Publish, Subscribe, QoS};
use async_channel::{bounded, Sender, Receiver, SendError, RecvError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to send cancel request to eventloop")]
    Cancel(#[from] SendError<()>),
    #[error("Failed to send mqtt requests to eventloop")]
    Request(#[from] SendError<Request>),
    #[error("Failed to recv mqtt notification from eventloop")]
    Notification(#[from] RecvError),
}

/// `Client` can communicate with MQTT eventloop `Connection`. Client is
/// cloneable and can be used to synchronously Publish, Subscribe as well as
/// receive incoming MQTT messages
#[derive(Clone)]
pub struct Client {
    request_tx: Sender<Request>,
    cancel_tx: Sender<()>,
}

impl Client {
    /// Create a new `Client`
    pub fn new(options: MqttOptions, cap: usize) -> (Client, Connection) {
        let (request_tx, request_rx) = bounded(cap);
        let (cancel_tx, cancel_rx) = bounded(cap);
        let eventloop = EventLoop::new(options, request_rx);
        let client = Client {
            request_tx,
            cancel_tx,
        };

        let connection = Connection::new(eventloop, cancel_rx);
        (client, connection)
    }

    /// Returns an asynchronous `Sender` to send MQTT requests to `Connection` eventloop
    pub fn async_requests(&self) -> async_channel::Sender<Request> {
        self.request_tx.clone()
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn publish<S, V>(&mut self, topic: S, qos: QoS, retain: bool, payload: V) -> Result<(), Error>
        where S: Into<String>, V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload.into());
        publish.retain = retain;
        let request = Request::Publish(publish);
        blocking::block_on(self.request_tx.send(request))?;
        Ok(())
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, topic: S, qos: QoS) -> Result<(), Error> {
        let subscribe = Subscribe::new(topic.into(), qos);
        let request = Request::Subscribe(subscribe);
        blocking::block_on(self.request_tx.send(request))?;
        Ok(())
    }

    /// Stops the eventloop right away
    pub fn cancel(&mut self) -> Result<(), Error> {
        blocking::block_on(self.cancel_tx.send(()))?;
        Ok(())
    }
}

///  MQTT connection. Maintains all the necessary state and automatically retries connections
/// in flaky networks.
pub struct Connection {
    cancel_rx: Receiver<()>,
    connection_retry_delay: Duration,
    eventloop: EventLoop<Receiver<Request>>,
}

impl Connection {
    fn new(eventloop: EventLoop<Receiver<Request>>, cancel_rx: Receiver<()>) -> Connection {
        Connection {
            eventloop,
            cancel_rx,
            connection_retry_delay: Duration::from_secs(1)
        }
    }

    async fn connect_or_cancel(&mut self) -> Result<(), EventLoopError> {
        // select here prevents cancel request from being blocked until connection request is
        // resolved. Returns with an error if connections fail continuously
        select! {
            o = connect_with_sleep(&mut self.eventloop, self.connection_retry_delay) => o,
            _ = self.cancel_rx.recv() => {
                Err(EventLoopError::Cancel)
            }
        }
    }

    pub fn iter(&mut self) -> Iter {
        let mut runtime = runtime::Builder::new();
        runtime.basic_scheduler().enable_io().enable_time();

        Iter {
            connection: self,
            runtime: runtime::Runtime::new().unwrap(),
            connected: false
        }
    }
}

pub struct Iter<'a> {
    connection: &'a mut Connection,
    runtime: runtime::Runtime,
    connected: bool
}

impl<'a> Iter<'a> {
    fn connect(&mut self) -> Result<(Option<Incoming>, Option<Outgoing>), EventLoopError> {
        let f = self.connection.connect_or_cancel();
        self.runtime.block_on(f)?;
        Ok((Some(Incoming::Connected), None))
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(Option<Incoming>, Option<Outgoing>), EventLoopError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.connected {
            return Some(self.connect())
        }

        let f = self.connection.eventloop.poll();
        Some(self.runtime.block_on(f))
    }
}

async fn connect_with_sleep(eventloop: &mut EventLoop<Receiver<Request>>, sleep: Duration) -> Result<(), EventLoopError> {
    match eventloop.connect().await {
        Ok(_) => Ok(()),
        Err(e) => {
            time::delay_for(sleep).await;
            Err(e)
        }
    }
}
