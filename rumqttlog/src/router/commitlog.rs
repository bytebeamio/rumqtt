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

    /// Returns all the topics in the commitlog along with their current offsets
    pub fn _topics_snapshot(&self) -> Vec<(String, (u64, u64))> {
        let mut out = Vec::new();
        for (topic, log) in self.logs.iter() {
            let offset = log.last_offset();
            out.push((topic.clone(), offset));
        }

        out
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

    pub fn last_offset(&mut self, topic: &str) -> Option<(u64, u64)> {
        let log = match self.logs.get_mut(topic) {
            Some(log) => log,
            None => return None
        };

        Some(log.last_offset())
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

    pub fn topics(&self) -> Option<(usize, Vec<String>)> {
        let topics = self.topics.clone();
        match topics.is_empty() {
            true => None,
            false => {
                let last_offset = topics.len() - 1;
                Some((last_offset, topics))
            }
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

type Topic = String;

/// Snapshots of topics grouped by cluster id
#[derive(Debug, Clone)]
pub struct Snapshots {
    snapshots: HashMap<Topic, [(u64, u64); 3]>
}

impl Snapshots {
    pub fn _new() -> Snapshots {
        Snapshots {
            snapshots: HashMap::new()
        }
    }

    fn _fill(&mut self, commitlog_id: usize, snapshot: Vec<(String, (u64, u64))>) {
        for (topic, offset) in snapshot.into_iter() {
            match self.snapshots.get_mut(&topic) {
                Some(offsets) => {
                    offsets[commitlog_id] = offset;
                }
                None => {
                    let mut offsets = [(0, 0); 3];
                    offsets[commitlog_id] = offset;
                    self.snapshots.insert(topic, offsets);
                }
            }
        }
    }
}
