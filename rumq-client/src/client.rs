//! This module offers a high level synchronous abstraction to async eventloop.
//! Uses channels internally to get `Requests` and send `Notifications`

use crate::{EventLoop, MqttOptions, Incoming, Request, EventLoopError};

use std::time::Duration;
use tokio::stream::{StreamExt, Stream};
use tokio::{time, select};
use rumq_core::mqtt4::{Publish, Subscribe, QoS};
use async_channel::{bounded, Sender, SendError, TrySendError, RecvError};

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
    cancel_rx: async_channel::Receiver<()>,
    max_connection_retries: Option<usize>,
    current_connection_count: usize,
    connection_retry_delay: Duration,
    eventloop: EventLoop<async_channel::Receiver<Request>>,
}

impl Connection {
    fn new(eventloop: EventLoop<async_channel::Receiver<Request>>, cancel_rx: async_channel::Receiver<()>) -> Connection {
        Connection {
            eventloop,
            cancel_rx,
            max_connection_retries: None,
            current_connection_count: 0,
            connection_retry_delay: Duration::from_secs(1)
        }
    }

    /// Set maximum number of continuous connection retries. `start` method returns
    /// with an error after this count
    pub fn set_max_connection_retries(&mut self, max: usize) {
        self.max_connection_retries = Some(max);
    }

    async fn connect_or_cancel(&mut self) -> Result<bool, EventLoopError> {
        // select here prevents cancel request from being blocked until connection request is
        // resolved. Returns with an error if connections fail continuously
        select! {
            o = connect_with_sleep(&mut self.eventloop, self.connection_retry_delay) => {
                match o {
                    Ok(stream) => {
                        self.current_connection_count = 0;
                        Ok(true)
                    }
                    Err(e) => match self.max_connection_retries {
                        Some(max) if self.current_connection_count > max => Err(e),
                        Some(_) | None => {
                            self.current_connection_count += 1;
                            Ok(false)
                        }
                    }
                }
            }
            _ = self.cancel_rx.recv() => {
                Err(EventLoopError::Cancel)
            }
        }
    }
}

async fn connect_with_sleep(
    eventloop: &mut EventLoop<async_channel::Receiver<Request>>,
    sleep: Duration
) -> Result<(), EventLoopError> {
    match eventloop.connect().await {
        Ok(stream) => Ok(()),
        Err(e) => {
            error!("Failed to connect. Error = {:?}", e);
            time::delay_for(sleep).await;
            Err(e)
        }
    }
}

pub struct Iter {

}
