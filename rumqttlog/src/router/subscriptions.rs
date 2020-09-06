use mqtt4bytes::{has_wildcards, matches, SubscribeTopic};
use std::collections::{HashMap, HashSet};
use std::mem;

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Debug)]
pub struct Subscription {
    /// Topics that this connection's coroutine tracks. There can be inconsitency
    /// with the coroutine due to pending topics request from connection coroutine
    /// Inconsistency is captured in `untracked_topics`
    pub(crate) topics: Vec<(String, u8, [(u64, u64); 3])>,
    /// Topics index to not add duplicates to topics
    topics_index: HashSet<String>,
    /// Concrete subscriptions on this topic
    concrete_subscriptions: HashMap<String, u8>,
    /// Wildcard subscriptions on this topic
    wild_subscriptions: Vec<(String, u8)>,
}

impl Subscription {
    pub fn new() -> Subscription {
        Subscription {
            topics: Vec::new(),
            topics_index: HashSet::new(),
            concrete_subscriptions: HashMap::new(),
            wild_subscriptions: Vec::new(),
        }
    }

    /// Topics which aren't sent to tracker yet
    pub fn topics(&mut self) -> Option<Vec<(String, u8, [(u64, u64); 3])>> {
        let topics = mem::replace(&mut self.topics, Vec::new());
        if topics.is_empty() {
            None
        } else {
            Some(topics)
        }
    }

    /// Extracts new topics from topics log (from offset in TopicsRequest) and matches
    /// them against subscriptions of this connection. Returns a TopicsReply if there
    /// are matches
    pub(crate) fn matched_topics(
        &mut self,
        topics: &[String],
    ) -> Option<Vec<(String, u8, [(u64, u64); 3])>> {
        for topic in topics {
            self.fill_matches(topic);
        }

        let topics = mem::replace(&mut self.topics, Vec::new());
        if topics.is_empty() {
            None
        } else {
            Some(topics)
        }
    }

    /// A new subscription should match all the existing topics. Tracker
    /// should track matched topics from current offset of that topic
    pub fn add_subscription(&mut self, filters: Vec<SubscribeTopic>, topics: Vec<String>) {
        for filter in filters {
            if has_wildcards(&filter.topic_path) {
                self.wild_subscriptions
                    .push((filter.topic_path.clone(), filter.qos as u8));
            } else {
                self.concrete_subscriptions
                    .insert(filter.topic_path.clone(), filter.qos as u8);
            }

            // Check and track matching topics from input
            for topic in topics.iter() {
                // ignore if the topic is already being tracked
                if self.topics_index.contains(topic) {
                    continue;
                }

                if matches(&topic, &filter.topic_path) {
                    self.topics_index.insert(topic.clone());
                    self.topics
                        .push((topic.clone(), filter.qos as u8, [(0, 0); 3]));
                    continue;
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
            return false;
        }

        // A concrete subscription match
        if let Some(qos) = self.concrete_subscriptions.get(topic) {
            self.topics_index.insert(topic.to_owned());
            self.topics.push((topic.to_owned(), *qos, [(0, 0); 3]));
            return true;
        }

        // Wildcard subscription match. We return after first match
        for (filter, qos) in self.wild_subscriptions.iter() {
            if matches(&topic, filter) {
                self.topics_index.insert(topic.to_owned());
                self.topics.push((topic.to_owned(), *qos, [(0, 0); 3]));
                return true;
            }
        }

        false
    }
}
