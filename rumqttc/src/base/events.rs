use std::{io, time::Duration};

use flume::{bounded, Receiver, Sender};
use tokio::runtime::{self, Runtime};
use tokio_stream::StreamExt;
use tokio_util::time::{delay_queue::Key, DelayQueue};

/// An Event Bus that allows consumers to poll it for events
/// and allows publishers to send events through it.
#[derive(Debug)]
pub struct EventsRx<T> {
    timers: DelayQueue<(usize, T)>,
    tx: Sender<(usize, T)>,
    rx: Receiver<(usize, T)>,
    runtime: Runtime,
}

impl<T> EventsRx<T> {
    /// Create a new Event Bus
    pub fn new(max_events: usize) -> Self {
        let (tx, rx) = bounded(max_events);
        EventsRx {
            timers: DelayQueue::new(),
            tx,
            rx,

            runtime: runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn add_timer(&mut self, id: usize, event: T, duration: Duration) -> Key {
        let _guard = self.runtime.enter();
        self.timers.insert((id, event), duration)
    }

    pub fn remove_timer(&mut self, key: Key) {
        self.timers.try_remove(&key);
    }

    pub fn reset_timer(&mut self, key: Key, timeout: Duration) {
        let _guard = self.runtime.enter();
        self.timers.reset(&key, timeout)
    }

    pub async fn timer_events(&mut self) -> (usize, T) {
        let v = self.timers.next().await.unwrap();
        v.into_inner()
    }

    pub async fn recv_non_timer_events(&mut self) -> (usize, T) {
        self.rx.recv_async().await.unwrap()
    }

    /// Poll the event bus for an event
    /// GOAL figure out how to do this in an non-blocking way
    /// THERE IS ONLY ONE OF THESE! If we want more, we'll need to replace it with a broadcast
    /// channel.
    pub fn poll(&mut self) -> Result<(usize, T), EventError> {
        self.runtime.block_on(async {
            tokio::select! {
                // TODO: Cover next
                Some(event) = self.timers.next() => {
                    Ok::<(usize, T), EventError>(event.into_inner())
                }
                event = self.rx.recv_async() => {
                    let event = event.unwrap();
                    Ok(event)
                }
            }
        })
    }

    pub async fn next(&mut self) -> Result<(usize, T), EventError> {
        tokio::select! {
            // TODO: Cover next
            Some(event) = self.timers.next() => {
                Ok::<(usize, T), EventError>(event.into_inner())
            }
            event = self.rx.recv_async() => {
                let event = event.unwrap();
                Ok(event)
            }
        }
    }

    pub fn timer_events_blocking(&mut self) -> Result<(usize, T), EventError> {
        self.runtime.block_on(async {
            let event = self.timers.next().await;
            match event {
                Some(event) => Ok(event.into_inner()),
                None => Err(EventError::TimerStreamDone),
            }
        })
    }

    pub fn producer(&self, id: usize) -> EventsTx<T> {
        EventsTx {
            id,
            tx: self.tx.clone(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Timer stream done")]
    TimerStreamDone,
    #[error("Send error")]
    SendError,
    #[error("Try send error")]
    TrySendError,
}

/// An Event Bus that allows consumers to poll it for events
/// and allows publishers to send events through it.
#[derive(Clone, Debug)]
pub struct EventsTx<T> {
    pub id: usize,
    pub tx: Sender<(usize, T)>,
}

impl<T> EventsTx<T> {
    pub fn send(&self, event: T) -> Result<(), EventError> {
        self.tx
            .send((self.id, event))
            .map_err(|_e| EventError::SendError)
    }

    pub fn try_send(&self, event: T) -> Result<(), EventError> {
        // NOTE: we should ideally return the failed value in TrySendError
        // flume::TrySendError(..) returns the value but we are ignoring
        // it here to avoid type mess
        self.tx
            .try_send((self.id, event))
            .map_err(|_e| EventError::TrySendError)
    }

    pub async fn send_async(&self, event: T) -> Result<(), EventError> {
        self.tx
            .send_async((self.id, event))
            .await
            .map_err(|_e| EventError::SendError)
    }
}
