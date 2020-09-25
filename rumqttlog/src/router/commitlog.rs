use std::collections::HashMap;
use std::io;

use super::bytes::Bytes;
use crate::volatile::Log;
use crate::Config;
use std::sync::Arc;

// TODO change config to Arc
pub(crate) struct CommitLog {
    id: usize,
    config: Arc<Config>,
    logs: HashMap<String, Log>,
}

impl CommitLog {
    pub fn new(config: Arc<Config>, id: usize) -> CommitLog {
        CommitLog {
            id,
            config,
            logs: HashMap::new(),
        }
    }

    /// Appends the record to correct commitlog and returns a boolean to indicate
    /// if this topic is new along with the offset of append
    pub fn append(&mut self, topic: &str, record: Bytes) -> io::Result<(bool, (u64, u64))> {
        // Entry instead of if/else?
        if let Some(log) = self.logs.get_mut(topic) {
            let offsets = log.append(record);
            Ok((false, offsets))
        } else {
            let max_segment_size = self.config.max_segment_size;
            let max_segment_count = self.config.max_segment_count;
            let mut log = Log::new(max_segment_size, max_segment_count);
            let offsets = log.append(record);
            self.logs.insert(topic.to_owned(), log);
            Ok((true, offsets))
        }
    }

    fn next_offset(&self, topic: &str) -> Option<(u64, u64)> {
        let log = match self.logs.get(topic) {
            Some(log) => log,
            None => return None,
        };

        Some(log.next_offset())
    }

    pub fn seek_offsets_to_end(&self, topics: &mut Vec<(String, u8, [(u64, u64); 3])>) {
        for (topic, _, offset) in topics.iter_mut() {
            if let Some(last_offset) = self.next_offset(topic) {
                offset[self.id] = last_offset;
            }
        }
    }

    pub fn readv(
        &mut self,
        topic: &str,
        in_segment: u64,
        in_offset: u64,
    ) -> io::Result<Option<(Option<u64>, u64, u64, Vec<Bytes>)>> {
        // Router during data request and notifications will check both
        // native and replica commitlog where this topic doesn't exist
        let log = match self.logs.get_mut(topic) {
            Some(log) => log,
            None => return Ok(None),
        };

        let (jump, segment, offset, data) = log.readv(in_segment, in_offset);

        // For debugging. Will be removed later
        // println!(
        //     "In: segment {} offset {}, Out: segment {} offset {}, Count {}",
        //     in_segment,
        //     in_offset,
        //     segment,
        //     offset,
        //     data.len()
        // );
        Ok(Some((jump, segment, offset, data)))
    }
}

/// A temporal list of unique new topics
#[derive(Debug)]
pub struct TopicLog {
    /// List of new topics
    topics: Vec<String>,
}

impl TopicLog {
    /// Create a new topic log
    pub fn new() -> TopicLog {
        TopicLog { topics: Vec::new() }
    }

    pub fn topics(&self) -> Vec<String> {
        self.topics.clone()
    }

    /// read n topics from a give offset along with offset of the last read topic
    pub fn readv(&self, offset: usize, count: usize) -> Option<(usize, &[String])> {
        let len = self.topics.len();
        if offset >= len || count == 0 {
            return None;
        }

        let mut next_offset = offset + count;
        if next_offset >= len {
            next_offset = len;
        }

        let out = self.topics[offset..next_offset].as_ref();
        if out.is_empty() {
            return None;
        }

        Some((next_offset, out))
    }

    /// Appends the topic if the topic isn't already seen
    pub fn append(&mut self, topic: &str) {
        self.topics.push(topic.to_owned());
    }
}
