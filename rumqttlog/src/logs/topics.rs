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

    /// read n topics from a give offset along with offset of the last read topic
    pub fn readv(&self, offset: usize, count: usize) -> Option<(usize, &[String])> {
        let len = self.topics.len();
        if offset >= len {
            return None;
        }

        // read till the end if the count is 0
        let mut next_offset = if count == 0 { len } else { offset + count };

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
