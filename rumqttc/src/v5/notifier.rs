use std::{
    collections::VecDeque,
    mem,
    sync::{Arc, Mutex},
};

use crate::v5::Incoming;

#[derive(Debug)]
pub struct Notifier {
    incoming_buf: Arc<Mutex<VecDeque<Incoming>>>,
    incoming_buf_cache: VecDeque<Incoming>,
}

impl Notifier {
    #[inline]
    pub(crate) fn new(
        incoming_buf: Arc<Mutex<VecDeque<Incoming>>>,
        incoming_buf_cache: VecDeque<Incoming>,
    ) -> Self {
        Self {
            incoming_buf,
            incoming_buf_cache,
        }
    }

    #[inline]
    pub fn iter(&mut self) -> NotifierIter<'_> {
        NotifierIter(self)
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
