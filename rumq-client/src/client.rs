//! This module offers a high level synchronous abstraction to async eventloop. Uses flume channels
//! to get `Requests` and send `Notifications`

use crate::{eventloop, MqttEventLoop, MqttOptions, Notification, Request};

use crossbeam_channel::TrySendError;
use std::time::Duration;
use tokio::stream::StreamExt;
use tokio::time;

pub struct Client {
    request_tx: flume::Sender<Request>,
    notification_rx: crossbeam_channel::Receiver<Notification>,
    notification_tx: Option<crossbeam_channel::Sender<Notification>>,
    cancel_tx: flume::Sender<()>,
    cancel_rx: Option<flume::Receiver<()>>,
    eventloop: MqttEventLoop,
}

impl Client {
    pub fn new(options: MqttOptions, cap: usize) -> Client {
        let (request_tx, request_rx) = flume::bounded(cap);
        let (notification_tx, notification_rx) = crossbeam_channel::bounded(cap);
        let (cancel_tx, cancel_rx) = flume::bounded(cap);
        let eventloop = eventloop(options, request_rx);
        Client {
            request_tx,
            notification_rx,
            notification_tx: Some(notification_tx),
            cancel_tx,
            cancel_rx: Some(cancel_rx),
            eventloop,
        }
    }

    /// Builds a channel like abstraction to send `Requests` to eventloop and receive `Notification`s
    /// from the eventloop
    pub fn channel(&self) -> (flume::Sender<Request>, crossbeam_channel::Receiver<Notification>) {
        (self.request_tx.clone(), self.notification_rx.clone())
    }

    pub fn cancel_handle(&self) -> flume::Sender<()> {
        self.cancel_tx.clone()
    }

    #[tokio::main(core_threads = 1)]
    pub async fn start(&mut self) {
        let mut last_failed = None;
        let notification_tx = self.notification_tx.take().unwrap();
        let cancel_rx = self.cancel_rx.take().unwrap();

        'reconnection: loop {
            let mut stream = self.eventloop.connect().await.unwrap();

            if let Some(item) = last_failed.take() {
                match notification_tx.try_send(item) {
                    Err(TrySendError::Full(failed)) => last_failed = Some(failed),
                    Err(TrySendError::Disconnected(failed)) => last_failed = Some(failed),
                    Ok(_) => (),
                }
            }

            while let Some(item) = stream.next().await {
                if cancel_rx.try_recv().is_ok() {
                    break 'reconnection
                }


                match notification_tx.try_send(item) {
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
    }
}



