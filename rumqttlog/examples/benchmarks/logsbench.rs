use std::fs;
use timestone::{CommitLog, Config};

use super::{
    generate_payload, DIR, MAX_SEGMENT_COUNT, MAX_SEGMENT_RECORDS, MAX_SEGMENT_SIZE, RECORD_COUNT, RECORD_SIZE, TOPICS_COUNT,
};
use std::path::PathBuf;

pub fn topics() -> Vec<String> {
    let mut topics = Vec::new();
    for i in 0..TOPICS_COUNT {
        let topic = format!("test/distributed/storage/{}", i);
        topics.push(topic);
    }

    topics
}


