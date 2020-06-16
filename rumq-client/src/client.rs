//! This module offers a high level synchronous abstraction to async eventloop.
//! Uses channels internally to get `Requests` and send `Notifications`

use crate::{eventloop, MqttEventLoop, MqttOptions, Notification, Request};

use crossbeam_channel::TrySendError;
use std::time::Duration;
    use std::thread::JoinHandle;
use tokio::stream::StreamExt;
use tokio::time;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::mpsc::error::SendError;
use rumq_core::mqtt4::{Publish, Subscribe, QoS};

use crate::eventloop::EventLoopError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to send cancel request to eventloop")]
    CancelError(#[from] SendError<()>),
    #[error("Failed to send mqtt requests to eventloop")]
    RequestError(#[from] SendError<Request>),
}

#[derive(Clone)]
pub struct Client {
    request_tx: Sender<Request>,
    notification_rx: crossbeam_channel::Receiver<Notification>,
    cancel_tx: Sender<()>,
}

impl Client {
    pub fn new(options: MqttOptions, cap: usize) -> (Client, Connection) {
        let (request_tx, request_rx) = channel(cap);
        let (notification_tx, notification_rx) = crossbeam_channel::bounded(cap);
        let (cancel_tx, cancel_rx) = channel(cap);
        let eventloop = eventloop(options, request_rx);
        let client = Client {
            request_tx,
            notification_rx,
            cancel_tx,
        };

        let connection = Connection { eventloop, notification_tx, cancel_rx };
        (client, connection)
    }

    pub fn notifications(&self) -> crossbeam_channel::Receiver<Notification> {
        self.notification_rx.clone()
    }

    /// Requests the eventloop for mqtt publish
    pub fn publish<S, V>(&mut self, topic: S, qos: QoS, retain: bool, payload: V) -> Result<(), Error>
        where
            S: Into<String>,
            V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload.into());
        publish.retain = retain;
        let request = Request::Publish(publish);
        futures_executor::block_on(self.request_tx.send(request))?;
        Ok(())
    }

    pub fn subscribe<S>(&mut self, topic: S, qos: QoS) -> Result<(), Error>
        where
            S: Into<String>,
    {
        let subscribe = Subscribe::new(topic.into(), qos);
        let request = Request::Subscribe(subscribe);
        futures_executor::block_on(self.request_tx.send(request))?;
        Ok(())
    }

    pub fn cancel(&mut self) -> Result<(), Error> {
        futures_executor::block_on(self.cancel_tx.send(()))?;
        Ok(())
    }
}

pub struct Connection {
    notification_tx: crossbeam_channel::Sender<Notification>,
    cancel_rx: Receiver<()>,
    eventloop: MqttEventLoop
}

impl Connection {
    #[tokio::main(core_threads = 1)]
    pub async fn start(&mut self) -> Result<(), EventLoopError>{
        let mut last_failed = None;
        'reconnection: loop {
            let mut stream = self.eventloop.connect().await?;
            if let Some(item) = last_failed.take() {
                match self.notification_tx.try_send(item) {
                    Err(TrySendError::Full(failed)) => last_failed = Some(failed),
                    Err(TrySendError::Disconnected(failed)) => last_failed = Some(failed),
                    Ok(_) => (),
                }
            }

            while let Some(item) = stream.next().await {
                if self.cancel_rx.try_recv().is_ok() {
                    break 'reconnection
                }

                match self.notification_tx.try_send(item) {
                    Err(TrySendError::Full(failed)) => {
                        last_failed = Some(failed);
                        break;
                    }
                    Err(TrySendError::Disconnected(failed)) => {
                        last_failed = Some(failed);
                        break;
                    }
                    Ok(_) => continue,
                }
            }

            time::delay_for(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    pub fn set_options(&mut self, opt: MqttOptions) {
        self.eventloop.options = opt;

    }
    /// Spawns a new thread to run the connection inside, this thread will consume the Connection
    /// object, but return it and an eventual error when the connection is aborted
    pub fn start_in_thread(mut self) -> JoinHandle<(Connection, Result<(), EventLoopError>)> {
        std::thread::spawn(move || {
            let e = self.start();
            (self, e)
        })
    }
}



