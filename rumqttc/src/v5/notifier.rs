use std::collections::VecDeque;
use std::mem;
use std::sync::{Arc, Mutex, RwLock};

use crate::v5::Incoming;

/// Error type to denote if Notifier failed to resolve due to empty buffer or due to disconnection
#[derive(Debug, Eq, PartialEq)]
pub enum TryRecvError {
    /// [`EventLoop`] is disconnected from broker
    Disconnected,
    /// [`EventLoop`] is connected to broker
    Waiting,
}

#[derive(Debug)]
pub struct Notifier {
    incoming_buf: Arc<Mutex<VecDeque<Incoming>>>,
    incoming_buf_cache: VecDeque<Incoming>,
    disconnected: Arc<RwLock<bool>>,
}

impl Notifier {
    #[inline]
    pub(crate) fn new(
        incoming_buf: Arc<Mutex<VecDeque<Incoming>>>,
        incoming_buf_cache: VecDeque<Incoming>,
        disconnected: Arc<RwLock<bool>>,
    ) -> Self {
        Self {
            incoming_buf,
            incoming_buf_cache,
            disconnected,
        }
    }

    #[inline]
    pub fn is_disconnected(&self) -> bool {
        *self.disconnected.read().unwrap()
    }

    #[inline]
    pub fn iter(&mut self) -> NotifierIter<'_> {
        NotifierIter(self)
    }

    #[inline]
    /// Returns None immediately even if not disconnected, use when expecting non-blocking interface
    pub fn try_recv(&mut self) -> Result<Incoming, TryRecvError> {
        let next = match self.incoming_buf_cache.pop_front() {
            None => {
                let mut incoming_buf = self.incoming_buf.lock().unwrap();
                if incoming_buf.is_empty() {
                    None
                } else {
                    mem::swap(&mut self.incoming_buf_cache, &mut *incoming_buf);
                    drop(incoming_buf);
                    self.incoming_buf_cache.pop_front()
                }
            }
            val => val,
        };

        match next {
            Some(p) => Ok(p),
            None if self.is_disconnected() => Err(TryRecvError::Disconnected),
            None => Err(TryRecvError::Waiting),
        }
    }
}

impl Iterator for Notifier {
    type Item = Incoming;

    #[inline]
    /// Return None only if disconnected, else block and retry till resolves as Some
    fn next(&mut self) -> Option<Incoming> {
        loop {
            match self.try_recv() {
                Ok(p) => return Some(p),
                Err(TryRecvError::Disconnected) => return None,
                _ => {}
            }
        }
    }
}

pub struct NotifierIter<'a>(&'a mut Notifier);

impl<'a> Iterator for NotifierIter<'a> {
    type Item = Incoming;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex, RwLock},
    };

    use super::{Notifier, TryRecvError};

    #[test]
    fn when_disconnected() {
        let mut notifier = Notifier::new(
            Arc::new(Mutex::new(VecDeque::new())),
            VecDeque::new(),
            Arc::new(RwLock::new(true)),
        );

        assert_eq!(notifier.try_recv(), Err(TryRecvError::Disconnected));

        assert_eq!(notifier.next(), None);
    }
}
