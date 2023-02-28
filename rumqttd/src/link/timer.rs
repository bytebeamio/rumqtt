use std::collections::HashMap;
use std::time::Duration;

use crate::{router::Event, MetricType};
use crate::{ConnectionId, MetricSettings};
use flume::{SendError, Sender};
use tokio::select;
use tracing::error;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Channel send error")]
    Send(#[from] SendError<(ConnectionId, Event)>),
    #[error("Timeout = {0}")]
    Elapsed(#[from] tokio::time::error::Elapsed),
}

pub async fn start(
    config: HashMap<MetricType, MetricSettings>,
    router_tx: Sender<(ConnectionId, Event)>,
) {
    let span = tracing::info_span!("metrics_timer");
    let _guard = span.enter();

    let mut alerts_push_interval = config
        .get(&MetricType::Alerts)
        .map(|interval| tokio::time::interval(Duration::from_secs(interval.push_interval)));

    let mut meters_push_interval = config
        .get(&MetricType::Meters)
        .map(|interval| tokio::time::interval(Duration::from_secs(interval.push_interval)));

    loop {
        select! {
            _ = alerts_push_interval.as_mut().unwrap().tick(), if alerts_push_interval.is_some() => {
                if let Err(e) = router_tx.send_async((0, Event::SendAlerts)).await {
                    error!("Failed to push alerts: {e}");
                }
            }
            _ = meters_push_interval.as_mut().unwrap().tick(), if meters_push_interval.is_some() => {
                if let Err(e) = router_tx.send_async((0, Event::SendMeters)).await {
                    error!("Failed to push alerts: {e}");
                }
            }
        }
    }
}
