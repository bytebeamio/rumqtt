use tokio::sync::oneshot;

use crate::{
    v5::mqttbytes::v5::{SubscribeReasonCode as V5SubscribeReasonCode, UnsubAckReason},
    SubscribeReasonCode,
};

#[derive(Debug, thiserror::Error)]
pub enum NoticeError {
    #[error("Eventloop dropped Sender")]
    Recv,
    #[error(" v4 Subscription Failure Reason Code: {0:?}")]
    V4Subscribe(SubscribeReasonCode),
    #[error(" v5 Subscription Failure Reason Code: {0:?}")]
    V5Subscribe(V5SubscribeReasonCode),
    #[error(" v5 Unsubscription Failure Reason: {0:?}")]
    V5Unsubscribe(UnsubAckReason),
}

impl From<oneshot::error::RecvError> for NoticeError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::Recv
    }
}

type NoticeResult = Result<(), NoticeError>;

/// A token through which the user is notified of the publish/subscribe/unsubscribe packet being acked by the broker.
#[derive(Debug)]
pub struct NoticeFuture(pub(crate) oneshot::Receiver<NoticeResult>);

impl NoticeFuture {
    /// Wait for broker to acknowledge by blocking the current thread
    ///
    /// # Panics
    /// Panics if called in an async context
    pub fn wait(self) -> NoticeResult {
        self.0.blocking_recv()?
    }

    /// Await the packet acknowledgement from broker, without blocking the current thread
    pub async fn wait_async(self) -> NoticeResult {
        self.0.await?
    }

    /// Attempts to check if the broker acknowledged the packet, without blocking the current thread
    /// or consuming the notice.
    ///
    /// It will return [`None`] if the packet wasn't acknowledged.
    ///
    /// Multiple calls to this functions can fail with [`NoticeError::Recv`] if the notice was
    /// already waited and the packet was already acknowledged and [`Some`] value was returned.
    pub fn try_wait(&mut self) -> Option<NoticeResult> {
        match self.0.try_recv() {
            Ok(res) => Some(res),
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(NoticeError::Recv)),
            Err(oneshot::error::TryRecvError::Empty) => None,
        }
    }
}

#[derive(Debug)]
pub struct NoticeTx(pub(crate) oneshot::Sender<NoticeResult>);

impl NoticeTx {
    pub fn new() -> (Self, NoticeFuture) {
        let (notice_tx, notice_rx) = tokio::sync::oneshot::channel();

        (NoticeTx(notice_tx), NoticeFuture(notice_rx))
    }

    pub(crate) fn success(self) {
        _ = self.0.send(Ok(()));
    }

    pub(crate) fn error(self, e: NoticeError) {
        _ = self.0.send(Err(e));
    }
}
