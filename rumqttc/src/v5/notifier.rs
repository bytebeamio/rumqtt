use std::{
    collections::VecDeque,
    mem,
    sync::{Arc, Mutex, RwLock},
};

use crate::v5::Incoming;

pub struct Disconnected;

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
    pub fn iter(&mut self) -> Result<NotifierIter<'_>, Disconnected> {
        if self.is_disconnected() {
            return Err(Disconnected);
        }

        Ok(NotifierIter(self))
    }
}

impl Iterator for Notifier {
    type Item = Incoming;

    #[inline]
    fn next(&mut self) -> Option<Incoming> {
        match self.incoming_buf_cache.pop_front() {
            None => {
                mem::swap(
                    &mut self.incoming_buf_cache,
                    &mut *self.incoming_buf.lock().unwrap(),
                );
                self.incoming_buf_cache.pop_front()
            }
            val => val,
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
