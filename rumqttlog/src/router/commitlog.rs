use std::collections::{HashMap, HashSet};
use std::io;

use super::bytes::Bytes;
use crate::volatile::Log;
use crate::Config;

// TODO change config to Arc
pub(crate) struct CommitLog {
    config: Config,
    logs: HashMap<String, Log>,
}

impl CommitLog {
    pub fn new(config: Config) -> CommitLog {
        CommitLog {
            config: config.clone(),
            logs: HashMap::new(),
        }
    }

    /// Appends the record to correct commitlog and returns a boolean to indicate
    /// if this topic is new along with the offset of append
    pub fn append(&mut self, topic: &str, record: Bytes) -> io::Result<(bool, u64)> {
        // Entry instead of if/else?
        if let Some(log) = self.logs.get_mut(topic) {
            let offset = log.append(record);
            Ok((false, offset))
        } else {
            let max_segment_size = self.config.max_segment_size;
            let max_segment_count = self.config.max_segment_count;
            let mut log = Log::new(max_segment_size, max_segment_count);
            let offset = log.append(record);
            self.logs.insert(topic.to_owned(), log);
            Ok((true, offset))
        }
    }

    pub fn readv(
        &mut self,
        topic: &str,
        segment: u64,
        offset: u64,
    ) -> io::Result<Option<(bool, u64, u64, Vec<Bytes>)>> {
        // Router during data request and notifications will check both
        // native and replica commitlog where this topic doesn't exist
        let log = match self.logs.get_mut(topic) {
            Some(log) => log,
            None => return Ok(None)
        };

        let (done, segment, offset, data) = log.readv(segment, offset);
        Ok(Some((done, segment, offset, data)))
    }
}

/// A temporal list of unique new topics
#[derive(Debug)]
pub struct TopicLog {
    /// Hashset of unique topics. Used to check if the topic is already seen
    unique: HashSet<String>,
    /// List of new topics
    topics: Vec<String>,
}

impl TopicLog {
    /// Create a new topic log
    pub fn new() -> TopicLog {
        TopicLog {
            unique: HashSet::new(),
            topics: Vec::new(),
        }
    }

    /// Appends the topic if the topic isn't already seen
    pub fn append(&mut self, topic: &str) {
        self.topics.push(topic.to_owned());
    }

    /// read n topics from a give offset along with offset of the last read topic
    pub fn readv(&self, offset: usize, count: usize) -> Option<(usize, Vec<String>)> {
        let len = self.topics.len();
        if offset >= len || count == 0 {
            return None;
        }

        let mut last_offset = offset + count;
        if last_offset >= len {
            last_offset = len;
        }

        let out = self.topics[offset..last_offset].to_vec();
        Some((last_offset - 1, out))
    }
}
