use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot::{self, error::TryRecvError};

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("Sender has nothing to send instantly")]
    Waiting,
    #[error("Sender side of channel was dropped")]
    Disconnected,
}

pub type NoResponse = ();

/// Resolves with [`Pkid`] used against packet when:
/// 1. Packet is acknowldged by the broker, e.g. QoS 1/2 Publish, Subscribe and Unsubscribe
/// 2. QoS 0 packet finishes processing in the [`EventLoop`]
pub struct Token<T> {
    rx: oneshot::Receiver<T>,
}

impl<T> Future for Token<T> {
    type Output = Result<T, TokenError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, TokenError>> {
        let polled = unsafe { self.map_unchecked_mut(|s| &mut s.rx) }.poll(cx);

        match polled {
            Poll::Ready(Ok(p)) => Poll::Ready(Ok(p)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(TokenError::Disconnected)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// There is a type of token returned for each type of [`Request`] when it is created and
/// sent to the [`EventLoop`] for further processing from the [`Client`]/[`AsyncClient`].
/// Some tokens such as those associated with the resolve with the `pkid` value used in the packet sent to the broker while other
/// tokens don't return such a value.
impl<T> Token<T> {
    /// Blocks on the current thread and waits till the packet completes being handled.
    ///
    /// ## Errors
    /// Returns [`TokenError::Disconnected`] if the [`EventLoop`] was dropped(usually),
    /// [`TokenError::Rejection`] if the packet acknowledged but not accepted.
    pub fn wait(self) -> Result<T, TokenError> {
        self.rx
            .blocking_recv()
            .map_err(|_| TokenError::Disconnected)
    }

    /// Attempts to check if the packet handling has been completed, without blocking the current thread.
    ///
    /// ## Errors
    /// Returns [`TokenError::Waiting`] if the packet wasn't acknowledged yet.
    /// Multiple calls to this functions can fail with [`TokenError::Disconnected`]
    /// if the promise has already been resolved.
    pub fn check(&mut self) -> Result<T, TokenError> {
        self.rx.try_recv().map_err(|e| match e {
            TryRecvError::Empty => TokenError::Waiting,
            TryRecvError::Closed => TokenError::Disconnected,
        })
    }
}

#[derive(Debug)]
pub struct Resolver<T> {
    tx: oneshot::Sender<T>,
}

impl<T> Resolver<T> {
    pub fn new() -> (Self, Token<T>) {
        let (tx, rx) = oneshot::channel();

        (Self { tx }, Token { rx })
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        let (tx, _) = oneshot::channel();

        Self { tx }
    }

    pub fn resolve(self, resolved: T) {
        if self.tx.send(resolved).is_err() {
            trace!("Promise was dropped")
        }
    }
}
