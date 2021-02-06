#[cfg(feature = "async-std-runtime")]
pub mod time {
use futures::Future;
use futures::future::FusedFuture;
use std::task::{Context, Poll};
use std::pin::Pin;
pub use std::time::Instant;
use async_io::Timer;

pub mod error {
    pub use async_std::future::TimeoutError as Elapsed;
}

pub use async_std::future::timeout;

pub struct Sleep {
    timer: Timer,
    terminated: bool,
}

pub fn sleep(duration: std::time::Duration) -> Sleep {
    Sleep {
        timer: Timer::after(duration),
        terminated: false,
    }
}

impl Sleep {
    pub fn reset(&mut self, instant: Instant) {
        self.timer.set_at(instant);
        self.terminated = false;
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.timer).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                self.terminated = true;
                Poll::Ready(())
            },
        }
    }
}

impl FusedFuture for Sleep {
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}


}

use std::task::{Context, Poll};
use std::pin::Pin;
use std::future::Future;
#[cfg(feature = "async-std-runtime")]
use futures::future::FusedFuture;
use pin_project::pin_project;

#[pin_project]
pub struct CondFuture<F> {
    #[pin]
    f: Option<F>
}

impl<F> CondFuture<F> {
    pub fn new(f: F, condition: bool) -> CondFuture<F> {
        if condition {
            CondFuture { f: Some(f) }
        } else {
            CondFuture { f: None }
        }
    }
}

impl<F: Future> Future for CondFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(f) = self.project().f.as_pin_mut() {
            f.poll(cx)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(feature = "async-std-runtime")]
impl<F: FusedFuture> FusedFuture for CondFuture<F> {
    fn is_terminated(&self) -> bool {
        if let Some(f) = &self.f {
            f.is_terminated()
        } else {
            true
        }
    }
}

pub fn cond_fut<F>(f: F, condition: bool) -> CondFuture<F> {
    CondFuture::new(f,condition)
}

#[cfg(feature = "async-std-runtime")]
pub fn fuse<F: Future>(f: F) -> impl Future<Output = F::Output> + FusedFuture {
    futures::FutureExt::fuse(f)
}

#[cfg(feature = "tokio-runtime")]
pub fn fuse<F>(f: F) -> F {
    f
}
