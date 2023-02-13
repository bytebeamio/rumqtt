use std::time::Duration;

use crate::router::Event;
use crate::ConnectionId;
use flume::{SendError, Sender};
use tokio::select;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Timeout = {0}")]
    Elapsed(#[from] tokio::time::error::Elapsed),
}

pub async fn start(router_tx: Sender<(ConnectionId, Event)>) {
    let mut alerts_push_interval = tokio::time::interval(Duration::from_secs(1));
    let mut meters_push_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        select! {
            _ = alerts_push_interval.tick() => {
                router_tx.send_async((0, Event::SendAlerts)).await;
            }
            _ = meters_push_interval.tick() => {
                router_tx.send_async((0, Event::SendMeters)).await;
            }
        }
    }
}
