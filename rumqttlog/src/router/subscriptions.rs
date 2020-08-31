use std::collections::{HashSet, HashMap};
use rumqttc::{has_wildcards, matches, SubscribeTopic};


/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
pub struct Subscription {
    /// Topics that this connection's coroutine tracks. There can be inconsitency
    /// with the coroutine due to pending topics request from connection coroutine
    /// Inconsistency is captured in `untracked_topics`
    pub(crate) topics: HashSet<String>,
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
            topics: HashSet::new(),
            untracked_existing_subscription_matches: Vec::new(),
            untracked_new_subscription_matches: Vec::new(),
            concrete_subscriptions: HashMap::new(),
            wild_subscriptions: Vec::new(),
        }
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

            for topic in topics.iter() {
                // ignore if the topic is already being tracked
                if self.topics.contains(topic) {
                    continue
                }

                if matches(&topic, &filter.topic_path) {
                    self.topics.insert(topic.clone());
                    self.untracked_new_subscription_matches.push((topic.clone(), filter.qos as u8, (0, 0)));
                    continue
                }
            }
        }

    }

    /// Matches existing subscription with a new topic. These
    /// topics should be tracked by tracker from offset 0
    pub fn fill_matches(&mut self, topic: String) {
        // ignore if the topic is already being tracked
        if self.topics.contains(&topic) {
            return
        }

        // A concrete subscription match
        if let Some(qos) = self.concrete_subscriptions.get(&topic) {
            self.topics.insert(topic.clone());
            self.untracked_existing_subscription_matches.push((topic, *qos, (0, 0)));
            return
        }

        // Wildcard subscription match
        for filter in self.wild_subscriptions.iter() {
            if matches(&topic, &filter.0) {
                self.topics.insert(topic.clone());
                self.untracked_existing_subscription_matches.push((topic, filter.1, (0, 0)));
                return
            }
        }
    }
}
