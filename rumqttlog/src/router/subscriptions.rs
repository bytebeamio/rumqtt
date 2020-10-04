use crate::router::{AcksRequest, Request, TopicsRequest};
use crate::DataRequest;
use mqtt4bytes::{has_wildcards, matches, SubscribeTopic};
use std::collections::{HashMap, HashSet, VecDeque};

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
#[derive(Debug)]
pub struct Tracker {
    /// Requests to pull data from commitlog
    requests: VecDeque<Request>,
    /// Topics index to not add duplicates to topics
    topics_index: HashSet<String>,
    /// Concrete subscriptions on this topic
    concrete_subscriptions: HashMap<String, u8>,
    /// Wildcard subscriptions on this topic
    wild_subscriptions: Vec<(String, u8)>,
    /// Topics Matches on new subscription waiting for offset updates
    matched: VecDeque<(String, u8, [(u64, u64); 3])>,
}

impl Tracker {
    pub fn new() -> Tracker {
        let mut requests = VecDeque::with_capacity(100);
        requests.push_back(Request::Acks(AcksRequest));

        Tracker {
            requests,
            topics_index: HashSet::new(),
            concrete_subscriptions: HashMap::new(),
            wild_subscriptions: Vec::new(),
            matched: VecDeque::with_capacity(100),
        }
    }

    /// Returns current number of subscriptions
    pub fn count(&self) -> usize {
        self.concrete_subscriptions.len() + self.wild_subscriptions.len()
    }

    pub fn pop_request(&mut self) -> Option<Request> {
        self.requests.pop_front()
    }

    pub fn register_data_request(&mut self, request: DataRequest) {
        let request = Request::Data(request);
        self.requests.push_back(request)
    }

    pub fn register_topics_request(&mut self, next_offset: usize) {
        let request = TopicsRequest::offset(next_offset);
        let request = Request::Topics(request);
        self.requests.push_back(request);
    }

    pub fn register_acks_request(&mut self) {
        let request = Request::Acks(AcksRequest);
        self.requests.push_back(request);
    }

    /// Match and add this topic to requests if it matches.
    /// Register new topics
    pub fn track_matched_topics(&mut self, topics: &[String]) -> usize {
        let mut matched_count = 0;
        for topic in topics {
            if self.match_with_subscriptions(topic) {
                let request = DataRequest::new(topic.to_owned());
                let request = Request::Data(request);
                self.requests.push_back(request);
                matched_count += 1;
            }
        }

        matched_count
    }

    /// Updates offsets and moves matches to the tracker
    pub fn next_matched(&mut self) -> Option<(String, u8, [(u64, u64); 3])> {
        self.matched.pop_front()
    }

    /// A new subscription should match all the existing topics. Tracker
    /// should track matched topics from current offset of that topic
    /// Adding and matching is combined so that only new subscriptions are
    /// matched against provided topics and then added to subscriptions
    pub fn add_subscription_and_match(&mut self, filters: Vec<SubscribeTopic>, topics: &[String]) {
        // Register topics request during first subscription
        if self.count() == 0 {
            let request = Request::Topics(TopicsRequest::offset(topics.len()));
            self.requests.push_back(request);
        }

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
                    let qos = filter.qos as u8;
                    self.matched.push_back((topic.clone(), qos, [(0, 0); 3]));
                    continue;
                }
            }
        }
    }

    /// Matches topic against existing subscriptions. These
    /// topics should be tracked by tracker from offset 0.
    /// Returns true if this topic matches a subscription for
    /// router to trigger new topic notification
    fn match_with_subscriptions(&mut self, topic: &str) -> bool {
        // ignore if the topic is already being tracked
        if self.topics_index.contains(topic) {
            return false;
        }

        // A concrete subscription match
        if let Some(_qos) = self.concrete_subscriptions.get(topic) {
            self.topics_index.insert(topic.to_owned());
            return true;
        }

        // Wildcard subscription match. We return after first match
        for (filter, _qos) in self.wild_subscriptions.iter() {
            if matches(&topic, filter) {
                self.topics_index.insert(topic.to_owned());
                return true;
            }
        }

        false
    }
}
