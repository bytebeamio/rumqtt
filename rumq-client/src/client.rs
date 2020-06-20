//! This module offers a high level synchronous abstraction to async eventloop.
//! Uses channels internally to get `Requests` and send `Notifications`

use crate::{ConnectionError, EventLoop, Incoming, MqttOptions, Outgoing, Request};

use async_channel::{bounded, Receiver, RecvError, SendError, Sender};
use mqtt4bytes::*;
use std::mem;
use std::time::Duration;
use tokio::runtime;
use tokio::runtime::Runtime;

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

        // create mqtt eventloop and take cancellation handle
        let mut runtime = runtime::Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .unwrap();
        let eventloop = EventLoop::new(options, request_rx);
        let mut eventloop = runtime.block_on(eventloop);
        let cancel_tx = eventloop.take_cancel_handle().unwrap();

        let client = Client {
            request_tx,
            cancel_tx,
        };

        let connection = Connection::new(eventloop, runtime);
        (client, connection)
    }

    /// Returns an asynchronous `Sender` to send MQTT requests to `Connection` eventloop
    pub fn async_requests(&self) -> async_channel::Sender<Request> {
        self.request_tx.clone()
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn publish<S, V>(
        &mut self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<(), Error>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
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
    eventloop: EventLoop<Receiver<Request>>,
    runtime: Option<Runtime>,
}

impl Connection {
    fn new(mut eventloop: EventLoop<Receiver<Request>>, runtime: Runtime) -> Connection {
        eventloop.set_reconnection_delay(Duration::from_secs(1));
        Connection {
            eventloop,
            runtime: Some(runtime),
        }
    }

    pub fn set_reconnection_delay(&mut self, delay: Duration) {
        self.eventloop.set_reconnection_delay(delay)
    }

    pub fn iter(&mut self) -> Iter {
        let runtime = self.runtime.take().unwrap();
        Iter {
            connection: self,
            runtime,
            connected: false,
        }
    }
}

pub struct Iter<'a> {
    connection: &'a mut Connection,
    runtime: runtime::Runtime,
    connected: bool,
}

impl<'a> Iter<'a> {
    fn connect(&mut self) -> Result<(Option<Incoming>, Option<Outgoing>), ConnectionError> {
        let f = self.connection.eventloop.connect_or_cancel();
        self.runtime.block_on(f)?;
        self.connected = true;
        Ok((Some(Incoming::Connected), None))
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(Option<Incoming>, Option<Outgoing>), ConnectionError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.connected {
            return match self.connect() {
                Ok(v) => Some(Ok(v)),
                // cancellation errors during connection should stop the iterator
                Err(ConnectionError::Cancel) => {
                    trace!("Cancellation request received while connecting");
                    None
                }
                Err(e) => Some(Err(e)),
            };
        }

        let f = self.connection.eventloop.poll();
        match self.runtime.block_on(f) {
            Ok(v) => Some(Ok(v)),
            // closing of request channel should stop the iterator
            Err(ConnectionError::RequestsDone) => {
                trace!("Done with requests");
                None
            }
            Err(ConnectionError::Cancel) => {
                trace!("Cancellation request received");
                None
            }
            Err(e) => {
                self.connected = false;
                Some(Err(e))
            }
        }
    }
}

impl<'a> Drop for Iter<'a> {
    fn drop(&mut self) {
        let runtime = runtime::Builder::new().basic_scheduler().build().unwrap();
        self.connection.runtime = Some(mem::replace(&mut self.runtime, runtime));
    }
}
