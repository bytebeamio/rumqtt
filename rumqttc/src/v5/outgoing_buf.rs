use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use crate::v5::Request;

#[derive(Debug)]
pub struct OutgoingBuf {
    pub(crate) buf: VecDeque<Request>,
    pub(crate) pkid_counter: u16,
    pub(crate) capacity: usize,
}

impl OutgoingBuf {
    #[inline]
    pub fn new(cap: usize) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            buf: VecDeque::with_capacity(cap),
            pkid_counter: 0,
            capacity: cap,
        }))
    }

    #[inline]
    pub fn increment_pkid(&mut self) -> u16 {
        self.pkid_counter = if self.pkid_counter == self.capacity as u16 {
            1
        } else {
            self.pkid_counter + 1
        };
        self.pkid_counter
    }
}
