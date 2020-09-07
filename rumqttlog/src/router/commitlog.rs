use std::collections::HashMap;
use std::io;

use super::bytes::Bytes;
use crate::volatile::Log;
use crate::Config;
use std::sync::Arc;

// TODO change config to Arc
pub(crate) struct CommitLog {
    config: Arc<Config>,
    logs: HashMap<String, Log>,
}

impl CommitLog {
    pub fn new(config: Arc<Config>) -> CommitLog {
        CommitLog {
            config,
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

    pub fn last_offset(&mut self, topic: &str) -> Option<(u64, u64)> {
        let log = match self.logs.get_mut(topic) {
            Some(log) => log,
            None => return None,
        };

        Some(log.last_offset())
    }

    pub fn readv(
        &mut self,
        topic: &str,
        segment: u64,
        offset: u64,
        max_count: usize,
    ) -> io::Result<Option<(Option<u64>, u64, u64, Vec<Bytes>)>> {
        // Router during data request and notifications will check both
        // native and replica commitlog where this topic doesn't exist
        let log = match self.logs.get_mut(topic) {
            Some(log) => log,
            None => return Ok(None),
        };

        let (jump, segment, offset, data) = log.readv(segment, offset, max_count);
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
        // dbg!(&self.topics, &self.concrete_subscriptions);
        let len = self.topics.len();
        if offset >= len || count == 0 {
            return None;
        }

        let mut last_offset = offset + count;
        if last_offset >= len {
            last_offset = len;
        }

        let out = self.topics[offset..last_offset].as_ref();
        if out.is_empty() {
            return None;
        }

        Some((last_offset - 1, out))
    }

    /// Appends the topic if the topic isn't already seen
    pub fn append(&mut self, topic: &str) {
        self.topics.push(topic.to_owned());
    }
}

type Topic = String;

/// Snapshots of topics grouped by cluster id
#[derive(Debug, Clone)]
pub struct Snapshots {
    snapshots: HashMap<Topic, [(u64, u64); 3]>,
}

impl Snapshots {
    pub fn _new() -> Snapshots {
        Snapshots {
            snapshots: HashMap::new(),
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
