use crate::router::TopicsRequest;
use crate::DataRequest;
use mqtt4bytes::{has_wildcards, matches, SubscribeTopic};
use std::collections::{HashMap, HashSet, VecDeque};

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Debug)]
pub struct Subscription {
    /// Flag used to notify pending subscription request
    pending_subscription_request: bool,
    /// Topics request
    topics_request: Option<TopicsRequest>,
    /// Requests to pull data from commitlog
    data_requests: VecDeque<DataRequest>,
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
            pending_subscription_request: false,
            topics_request: None,
            data_requests: VecDeque::with_capacity(100),
            topics_index: HashSet::new(),
            concrete_subscriptions: HashMap::new(),
            wild_subscriptions: Vec::new(),
        }
    }

    pub fn pop_data_request(&mut self) -> Option<DataRequest> {
        self.data_requests.pop_front()
    }

    pub fn push_data_request(&mut self, request: DataRequest) {
        self.data_requests.push_back(request)
    }

    /// Returns current number of subscriptions
    pub fn count(&self) -> usize {
        self.concrete_subscriptions.len() + self.wild_subscriptions.len()
    }

    pub fn pending_subscription_request(&self) -> bool {
        self.pending_subscription_request
    }

    pub fn register_pending_subscription_request(&mut self) {
        self.pending_subscription_request = true;
    }

    pub fn register_topics_request(&mut self, next_offset: usize) {
        let request = TopicsRequest::offset(next_offset);
        self.topics_request = Some(request);
    }

    /// Match and add this topic to requests if it matches.
    /// Register new topics
    pub fn track_matched_topics(&mut self, topics: &[String]) -> usize {
        let mut matched_count = 0;
        for topic in topics {
            if self.track_if_matched(topic) {
                matched_count += 1;
            }
        }

        matched_count
    }

    /// A new subscription should match all the existing topics. Tracker
    /// should track matched topics from current offset of that topic
    pub fn add_subscription_and_match(
        &mut self,
        filters: Vec<SubscribeTopic>,
        topics: &[String],
    ) -> Vec<(String, u8, [(u64, u64); 3])> {
        let mut out = Vec::new();
        for filter in filters {
            if has_wildcards(&filter.topic_path) {
                let subscription = filter.topic_path.clone();
                let qos = filter.qos as u8;
                self.wild_subscriptions.push((subscription, qos));
            } else {
                let subscription = filter.topic_path.clone();
                let qos = filter.qos as u8;
                self.concrete_subscriptions.insert(subscription, qos);
            }

            // Check and track matching topics from input
            for topic in topics.iter() {
                // ignore if the topic is already being tracked
                if self.topics_index.contains(topic) {
                    continue;
                }

                if matches(&topic, &filter.topic_path) {
                    self.topics_index.insert(topic.clone());
                    out.push((topic.clone(), filter.qos as u8, [(0, 0); 3]));
                    continue;
                }
            }
        }

        out
    }

    /// Matches topic against existing subscriptions. These
    /// topics should be tracked by tracker from offset 0.
    /// Returns true if this topic matches a subscription for
    /// router to trigger new topic notification
    fn track_if_matched(&mut self, topic: &str) -> bool {
        // ignore if the topic is already being tracked
        if self.topics_index.contains(topic) {
            return false;
        }

        // A concrete subscription match
        if let Some(_qos) = self.concrete_subscriptions.get(topic) {
            self.topics_index.insert(topic.to_owned());
            let request = DataRequest::new(topic.to_owned());
            self.data_requests.push_back(request);
            return true;
        }

        // Wildcard subscription match. We return after first match
        for (filter, _qos) in self.wild_subscriptions.iter() {
            if matches(&topic, filter) {
                self.topics_index.insert(topic.to_owned());
                let request = DataRequest::new(topic.to_owned());
                self.data_requests.push_back(request);
                return true;
            }
        }

        false
    }
}
