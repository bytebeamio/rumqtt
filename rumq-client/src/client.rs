//! This module offers a high level synchronous abstraction to async eventloop. Uses flume channels
//! to get `Requests` and send `Notifications`

use crate::{MqttOptions, Request, Notification, eventloop, MqttEventLoop};

use flume::{Sender, Receiver, TrySendError};
use tokio::stream::StreamExt;
use tokio::time;
use std::thread;
use std::time::Duration;

/// Builds a channel like abstraction to send `Requests` to eventloop and receive `Notification`s
/// from the eventloop
pub fn channel(options: MqttOptions, cap: usize) -> (Sender<Request>, Receiver<Notification>) {
    let (request_tx, request_rx) = flume::bounded(cap);
    let (notificaiton_tx, notification_rx) = flume::bounded(cap);

    let e = eventloop(options, request_rx);
    thread::spawn(move || {
        start(e, notificaiton_tx)  ;
    });

    (request_tx, notification_rx)
}

#[tokio::main(core_threads = 1)]
async fn start(mut eventloop: MqttEventLoop, notification_tx: Sender<Notification>) {

    let mut last_failed = None;
    loop {
        let mut stream = eventloop.connect().await.unwrap();

        if let Some(item) = last_failed.take() {
            match notification_tx.try_send(item) {
                Err(TrySendError::Full(failed)) => last_failed = Some(failed),
                Err(TrySendError::Disconnected(failed)) => last_failed = Some(failed),
                Ok(_) => ()
            }
        }

        while let Some(item) = stream.next().await {
            match notification_tx.try_send(item) {
                Err(TrySendError::Full(failed)) => {
                    last_failed = Some(failed);
                    break
                },
                Err(TrySendError::Disconnected(failed)) => {
                    last_failed = Some(failed);
                    break
                },
                Ok(_) => continue
            }
        }

        time::delay_for(Duration::from_secs(3)).await;
    }

}