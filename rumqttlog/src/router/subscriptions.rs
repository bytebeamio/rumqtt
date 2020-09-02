use std::collections::{HashSet, HashMap};
use mqtt4bytes::{has_wildcards, matches, SubscribeTopic};


/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
pub struct Subscription {
    /// Topics that this connection's coroutine tracks. There can be inconsitency
    /// with the coroutine due to pending topics request from connection coroutine
    /// Inconsistency is captured in `untracked_topics`
    topics: Vec<String>,
    /// Topics index to not add duplicates to topics
    topics_index: HashSet<String>,
    /// New topics on existing subscriptions which aren't being tracked yet.
    /// This is a result of new topic matches on existing subscriptions.
    pub(crate) untracked_existing_subscription_matches: Vec<(String, u8, (u64, u64))>,
    /// New subscription matches on existing topics
    /// This is a result of subscriptions before next topics request
    pub(crate) untracked_new_subscription_matches: Vec<(String, u8, (u64, u64))>,
    /// Concrete subscriptions on this topic
    pub(crate) concrete_subscriptions: HashMap<String, u8>,
    /// Wildcard subscriptions on this topic
    pub(crate) wild_subscriptions: Vec<(String, u8)>,
}

impl Subscription {
    pub fn new() -> Subscription {
        Subscription {
            topics: Vec::new(),
            topics_index: HashSet::new(),
            untracked_existing_subscription_matches: Vec::new(),
            untracked_new_subscription_matches: Vec::new(),
            concrete_subscriptions: HashMap::new(),
            wild_subscriptions: Vec::new(),
        }
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

    /// A new subscription should match all the existing topics. Tracker
    /// should track matched topics from current offset of that topic
    pub fn add_subscription(&mut self, filters: Vec<SubscribeTopic>, topics: Vec<String>) {
        for filter in filters {
            if has_wildcards(&filter.topic_path) {
                self.wild_subscriptions.push((filter.topic_path.clone(), filter.qos as u8));
            } else {
                self.concrete_subscriptions.insert(filter.topic_path.clone(), filter.qos as u8);
            }

            // Check and track matching topics from input
            for topic in topics.iter() {
                // ignore if the topic is already being tracked
                if self.topics_index.contains(topic) {
                    continue
                }

                if matches(&topic, &filter.topic_path) {
                    self.topics_index.insert(topic.clone());
                    self.topics.push(topic.clone());
                    self.untracked_new_subscription_matches.push((topic.clone(), filter.qos as u8, (0, 0)));
                    continue
                }
            }
        }

    }

    /// Matches existing subscription with a new topic. These
    /// topics should be tracked by tracker from offset 0.
    /// Returns to if this topic matches a subscription for
    /// router to trigger new topic notification
    pub fn fill_matches(&mut self, topic: &str) -> bool {
        // ignore if the topic is already being tracked
        if self.topics_index.contains(topic) {
            return false
        }

        // A concrete subscription match
        if let Some(qos) = self.concrete_subscriptions.get(topic) {
            self.topics_index.insert(topic.to_owned());
            self.untracked_existing_subscription_matches.push((topic.to_owned(), *qos, (0, 0)));
            self.topics.push(topic.to_owned());
            return true
        }

        // Wildcard subscription match. We return after first match
        for filter in self.wild_subscriptions.iter() {
            if matches(&topic, &filter.0) {
                self.topics_index.insert(topic.to_owned());
                self.untracked_existing_subscription_matches.push((topic.to_owned(), filter.1, (0, 0)));
                self.topics.push(topic.to_owned());
                return true
            }
        }

        false
    }
}
