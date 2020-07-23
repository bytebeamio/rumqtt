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

    pub fn append(&mut self, topic: &str, record: Bytes) -> io::Result<u64> {
        // Entry instead of if/else?
        if let Some(log) = self.logs.get_mut(topic) {
            let offset = log.append(record)?;
            Ok(offset)
        } else {
            let max_segment_size = self.config.max_segment_size;
            let max_segment_count = self.config.max_segment_count;
            let mut log = Log::new(max_segment_size, max_segment_count)?;
            let offset = log.append(record)?;
            self.logs.insert(topic.to_owned(), log);
            Ok(offset)
        }
    }

    pub fn readv(
        &mut self,
        topic: &str,
        segment: u64,
        offset: u64,
        size: usize,
    ) -> io::Result<Option<(bool, u64, u64, usize, Vec<u64>, Vec<Bytes>)>> {
        let log = match self.logs.get_mut(topic) {
            Some(l) => l,
            None => return Ok(None),
        };

        let (done, segment, offset, total_size, ids, data) = log.readv(segment, offset, size)?;
        Ok(Some((done, segment, offset, total_size, ids, data)))
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
    pub fn unique_append(&mut self, topic: &str) -> bool {
        let mut append = false;
        if !self.unique.contains(topic) {
            self.topics.push(topic.to_owned());
            append = true;
        }

        self.unique.insert(topic.to_owned());
        append
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
