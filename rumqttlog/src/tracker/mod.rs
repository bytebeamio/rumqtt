use std::collections::{HashSet, VecDeque};

use crate::router::{TopicsReply, TopicsRequest, AcksReply, AcksRequest};
use crate::{DataReply, DataRequest, RouterInMessage};
use mqtt4bytes::{has_wildcards, matches};

/// Tracker tracks current offsets of all the subscriptions of a connection
/// It also updates the iterator list to efficiently iterate over all
/// the topics without having to create an iterator and thus locking itself
/// to get updated. Instead tracker just returns number of topics it currently
/// host along with the status of the topic (active/inactive) for link
/// to use while iterating. This helps us prevent maintaining 2 lists
/// (active, inactive)
#[derive(Debug)]
pub struct Tracker {
    /// A ring buffer which tracks
    /// new topics: TopicsRequest
    /// data for a given topic: DataRequest
    /// acks for incoming publishes: AcksRequest
    tracker: VecDeque<RouterInMessage>,
    /// List of topics that are being tracked for data. This
    /// index is used to not add topics which are are already
    /// being tracked by `data_requests`
    data_topics: HashSet<String>,
    /// List of topics that are already being tracked for watermarks.
    /// Index to not add topics which are already being tracked
    watermark_topics: HashSet<String>,
    /// List of concrete subscriptions
    concrete_subscriptions: HashSet<String>,
    /// List of wildcard subscriptions
    wild_subscriptions: Vec<String>,
    /// number of inflight requests
    inflight: usize,
    /// max message count in data request
    max_count: usize
}

impl Tracker {
    pub fn new(max_count: usize) -> Tracker {
        // TODO: Don't allow more than allocated capacity in tracker
        Tracker {
            tracker: VecDeque::with_capacity(100),
            data_topics: HashSet::new(),
            watermark_topics: HashSet::new(),
            concrete_subscriptions: HashSet::new(),
            wild_subscriptions: Vec::new(),
            inflight: 0,
            max_count
        }
    }

    pub fn has_next(&self) -> bool {
        if !self.tracker.is_empty() {
            return true
        }

        false
    }

    pub fn inflight(&self) -> usize {
        self.inflight
    }

    pub fn add_subscription(&mut self, filter: &str) {
        if has_wildcards(filter) {
            self.wild_subscriptions.push(filter.to_owned());
        } else {
            self.concrete_subscriptions.insert(filter.to_owned());
        }

        let request = RouterInMessage::AllTopicsRequest;
        self.tracker.push_front(request);
    }

    /// Adds a new topic to watermarks tracker
    pub fn track_watermark(&mut self, topic: &str) {
        // ignore if the topic is already being tracked
        if self.watermark_topics.contains(topic) {
            return;
        }

        let topic = topic.to_owned();
        self.watermark_topics.insert(topic.clone());
        let request = AcksRequest::new(topic, 0);
        let request = RouterInMessage::AcksRequest(request);
        self.tracker.push_back(request);
    }

    /// Match this topic to subscriptions this connection is interested in.
    /// Matches only if the topic isn't already tracked.
    /// initialized = true pulls data from commitlog from scratch. Used when
    /// there are new topics on existing subscription
    /// initialized = false pulls data from current offset. Used when there
    /// is a new subscription which matches existing topics in the commitlog
    fn match_and_track(&mut self, topic: String, initialized: bool) {
        // ignore if the topic is already being tracked
        if self.data_topics.contains(&topic) {
            return;
        }

        // Adds a new topic to track. Duplicates are already covered by 'match_subscriptions'
        // A concrete subscription match. Add this new topic to data tracker
        if self.concrete_subscriptions.contains(&topic) {
            self.data_topics.insert(topic.clone());
            let request = match initialized {
                true => DataRequest::offsets(topic, [(0, 0); 3]),
                false => DataRequest::new(topic),
            };

            // Prioritize new matched topics
            let request = RouterInMessage::DataRequest(request);
            self.tracker.push_front(request);
            return;
        }

        for filter in self.wild_subscriptions.iter() {
            if matches(&topic, filter) {
                self.data_topics.insert(topic.clone());
                let request = match initialized {
                    true => DataRequest::offsets(topic, [(0, 0); 3]),
                    false => DataRequest::new(topic),
                };
                let request = RouterInMessage::DataRequest(request);
                self.tracker.push_front(request);
                return;
            }
        }
    }

    pub fn update_watermarks_tracker(&mut self, reply: &AcksReply) {
        self.inflight -= 1;
        let request = AcksRequest::new(reply.topic.clone(), reply.offset);
        let request = RouterInMessage::AcksRequest(request);
        self.tracker.push_back(request);
    }

    /// Updates data tracker to track new topics in the commitlog if they match
    /// a subscription.So, a TopicReply triggers DataRequest
    pub fn track_new_topics(&mut self, reply: &TopicsReply) {
        self.inflight -= 1;
        for topic in reply.topics.iter() {
            // Adds a DataRequest to data tracker if there is a match
            self.match_and_track(topic.clone(), true);
        }

        let request = TopicsRequest::offset(reply.offset);
        let request = RouterInMessage::TopicsRequest(request);
        self.tracker.push_back(request);
    }

    /// Updates data tracker to track all the topics in the commitlog with new
    /// subscriptions.So, a TopicReply triggers DataRequest
    pub fn track_all_topics(&mut self, reply: &TopicsReply) {
        self.inflight -= 1;
        for topic in reply.topics.iter() {
            // Adds a DataRequest to data tracker if there is a match
            self.match_and_track(topic.clone(), false);
        }

        let request = TopicsRequest::offset(reply.offset);
        let request = RouterInMessage::TopicsRequest(request);
        self.tracker.push_back(request);
    }

    /// Updates offset of this topic for the next data request
    /// Note that we don't support removing a topic. Topics are only disabled when they
    /// are caught up. We don't remove even topics for unsubscribes at the moment.
    /// This allows for index of the tracker vector to not be invalidated when router
    /// holds a pending notification (be it immediate reply and notification when there
    /// is new data)
    pub fn update_data_tracker(&mut self, reply: &DataReply) {
        self.inflight -= 1;
        let request = DataRequest::offsets(
            reply.topic.clone(),
            reply.cursors
        );

        let request = RouterInMessage::DataRequest(request);
        self.tracker.push_back(request);
    }

    /// Returns data request from next topic by cycling through all the tracks
    /// `next()` is only called by link only after it receives a reply of previous topic
    /// This ignores inactive indexes while iterating
    /// If all the tracks are pending, this returns `None` indicating that link should stop
    /// making any new requests to the router
    pub fn next(&mut self) -> Option<RouterInMessage> {
        self.inflight += 1;
        self.tracker.pop_front()
    }
}

#[cfg(test)]
mod tests {
    /*
    use crate::router::{TopicsReply, AcksReply};
    use crate::tracker::Tracker;
    use crate::{DataReply, RouterInMessage};
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[test]
    fn next_track_iterates_through_tracks_correctly() {
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");

        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.match_and_track(topic.clone());
            tracker.track_watermark(&topic);
        }

        // 10 data requests
        for i in 0..10 {
            let topic = get_data_topic(&tracker.next().unwrap());
            let expected = format!("{}", i);
            assert_eq!(topic, expected);
        }

        // 10 watermark requests
        for i in 0..10 {
            let topic = get_watermarks_topic(&tracker.next().unwrap());
            let expected = format!("{}", i);
            assert_eq!(topic, expected);
        }

        // 1 topics request
        let message = tracker.next().unwrap();
        assert!(is_topics_request(&message));

        // now add a new topics to the tracker
        for i in 10..20 {
            let topic = format!("{}", i);
            tracker.match_and_track(topic.clone());
            tracker.track_watermark(&topic);
        }

        // 20 data requests
        for i in 10..20 {
            let topic = get_data_topic(&tracker.next().unwrap());
            let expected = format!("{}", i);
            assert_eq!(topic, expected);
        }

        for i in 10..20 {
            let topic = get_watermarks_topic(&tracker.next().unwrap());
            let expected = format!("{}", i);
            assert_eq!(topic, expected);
        }
    }


    #[test]
    fn all_inactive_topics_should_return_none() {
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");

        let message = tracker.next().unwrap();
        assert!(is_topics_request(&message));

        // no topics to track
        assert!(tracker.next().is_none());
        assert_eq!(tracker.active, false);
    }

    fn filled_data_reply(topic: &str, tracker_topic_offset: usize) -> DataReply {
        let reply = DataReply {
            done: false,
            topic: topic.to_owned(),
            native_segment: 0,
            native_offset: 1,
            native_count: 1,
            replica_segment: 0,
            replica_offset: 0,
            replica_count: 0,
            pkids: vec![],
            payload: vec![Bytes::from(vec![1, 2, 3])],
        };

        reply
    }

    fn filled_watermarks_reply(topic: &str, tracker_topic_offset: usize) -> AcksReply {
        let reply = AcksReply {
            topic: topic.to_owned(),
            pkids: VecDeque::new(),
            offset: 3,
        };

        reply
    }

    fn filled_topics_reply() -> TopicsReply {
        let reply = TopicsReply {
            offset: 1,
            topics: vec!["hello/world".to_owned()],
        };

        reply
    }

    fn get_data_topic(message: &RouterInMessage) -> String {
        match message {
            RouterInMessage::DataRequest(request) => request.topic.clone(),
            v => panic!("Expecting data request. Received = {:?}", v),
        }
    }

    fn get_watermarks_topic(message: &RouterInMessage) -> String {
        match message {
            RouterInMessage::AcksRequest(request) => request.topic.clone(),
            v => panic!("Expecting data request. Received = {:?}", v),
        }
    }

    fn is_topics_request(message: &RouterInMessage) -> bool {
        match message {
            RouterInMessage::TopicsRequest(_) => true,
            v => panic!("Expecting data request. Received = {:?}", v),
        }
    }

     */
}
