use std::collections::VecDeque;

use crate::Size;

#[derive(Default, Debug, Clone)]
pub struct Block<T> {
    pub id: usize,
    pub node_id: usize,
    pub compute_id: usize,
    pub io_id: usize,
    pub filter: String,
    pub filter_idx: Option<usize>,
    pub max_qos: u8,
    pub next_cursor: u64,
    pub current_payload_size: usize,
    pub max_payload_size: usize,
    pub max_payload_count: usize,
    pub payload: VecDeque<T>,
}

impl<T> Block<T> {
    pub fn new(id: usize) -> Self {
        Self {
            id,
            node_id: 0,
            compute_id: 0,
            io_id: 0,
            filter: String::new(),
            filter_idx: None,
            next_cursor: i64::MAX as u64,
            payload: VecDeque::with_capacity(100),
            current_payload_size: 0,
            max_payload_count: 100,
            max_payload_size: 100 * 1024,
            max_qos: 0,
        }
    }

    pub fn clear(&mut self) {
        self.payload.clear();
        self.current_payload_size = 0;
    }

    pub fn reset(&mut self) {
        self.clear();
        self.next_cursor = i64::MAX as u64;
        self.compute_id = 0;
        self.filter_idx = None;
        self.filter.clear();
        self.max_qos = 0;
    }

    pub fn with_filter(id: usize, compute_id: usize, filter: impl Into<String>) -> Self {
        Self {
            filter: filter.into(),
            compute_id,
            ..Self::new(id)
        }
    }

    pub fn with_filter_and_cursor(
        id: usize,
        compute_id: usize,
        filter: impl Into<String>,
        cursor: u64,
    ) -> Self {
        Self {
            filter: filter.into(),
            next_cursor: cursor,
            compute_id,
            ..Self::new(id)
        }
    }
}

impl<T: Size> Size for Block<T> {
    fn size(&self) -> usize {
        let mut total_size = 0;
        for p in self.payload.iter() {
            total_size += p.size();
        }

        total_size
    }
}
