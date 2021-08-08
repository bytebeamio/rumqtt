use std::ops::Add;
use std::time::Duration;

use rand::Rng;
use tokio::time::{error::Elapsed, Instant};

/// Mqtt reconnection strategy
pub trait ReconnectionStrategy {
    /// Callback function invoked after the connection is correcly established
    /// and connack is received
    fn on_connection_established(&mut self) {}

    /// Callback function invoked when a connection error happens and therefore connection is lost
    fn on_connection_failed(&mut self) {}

    /// Compute and return the instant when a new connection attempt could be performed
    fn next_attempt(&self) -> Instant;
}

/// Truncated exponential backoff reconnection strategy
/// Delay increases exponentially with a random delay ([0, 1] secs) to avoid synchronizations
pub struct TruncatedExponentialBackoffReconnectionStrategy {
    connection_stable_threshold: Duration,
    maximum_backoff: Duration,
    connection_established_ts: Option<Instant>,
    last_connection_failed_ts: Option<Instant>,
    attempts: u32,
}

const EXPONENTIAL_BACKOFF_MAX_ATTEMPTS: u32 = 40;

impl TruncatedExponentialBackoffReconnectionStrategy {
    /// New `TruncatedExponentialBackoffReconnectionStrategy`
    ///
    /// * `connection_stable_threshold` - If connection is lost before this duration is elapsed,
    ///    backoff algorithm won't be resetted
    /// * `maximum_backoff` - Maximum delay allowed
    pub fn new(
        connection_stable_threshold: Duration,
        maximum_backoff: Duration,
    ) -> Self {
        if maximum_backoff.gt(&Duration::from_secs(2_u64.pow(EXPONENTIAL_BACKOFF_MAX_ATTEMPTS - 1) + 1)) {
            warn!("Maximum backoff will never be reached. Attempts are capped to {}", EXPONENTIAL_BACKOFF_MAX_ATTEMPTS);
        }
        Self {
            connection_stable_threshold,
            maximum_backoff,
            connection_established_ts: None,
            last_connection_failed_ts: None,
            attempts: 0
        }
    }

    fn is_connection_stable(&self) -> bool {
        self.connection_established_ts
            .filter(|ts| ts.add(self.connection_stable_threshold).lt(&Instant::now()))
            .is_some()
    }
}

impl ReconnectionStrategy for TruncatedExponentialBackoffReconnectionStrategy {
    fn on_connection_established(&mut self) {
        self.connection_established_ts = Some(Instant::now());
    }

    fn on_connection_failed(&mut self) {
        if self.is_connection_stable() {
            self.attempts = 0;
        } else if self.attempts < EXPONENTIAL_BACKOFF_MAX_ATTEMPTS { // cap max attempts
            self.attempts += 1;
        }

        self.connection_established_ts = None;
        self.last_connection_failed_ts = Some(Instant::now());
    }

    fn next_attempt(&self) -> Instant {
        let backoff_delay = if self.attempts > 0 {
            std::cmp::min(
                Duration::from_millis(2_u64.pow(self.attempts - 1) * 1000 + rand::thread_rng().gen_range(0..1000)),
                self.maximum_backoff,
            )
        } else {
            Duration::ZERO
        };

        debug!("reconnection attempt: {} - delay {:?} millis", &self.attempts, &backoff_delay);
        if let Some(last_connection_failed_ts) = self.last_connection_failed_ts {
            last_connection_failed_ts.add(backoff_delay)
        } else {
            Instant::now()
        }
    }
}