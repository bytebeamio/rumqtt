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
    // TODO Replace this with vec later and use only pop?
    /// List of topics that we are tracking for data
    data_tracker: VecDeque<DataRequest>,
    /// List of topics that we are tracking for watermark
    watermarks_tracker: VecDeque<AcksRequest>,
    /// List of topics that we are tracking for watermark
    topics_tracker: Option<TopicsRequest>,
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
    /// Current tracker type. Data = 0, Watermarks = 1
    tracker_type: u8,
    /// flag to indicate if all the trackers are inactive
    pub(crate) active: bool
}

impl Tracker {
    pub fn new() -> Tracker {
        Tracker {
            data_tracker: VecDeque::with_capacity(100),
            watermarks_tracker: VecDeque::with_capacity(100),
            topics_tracker: Some(TopicsRequest::new()),
            data_topics: HashSet::new(),
            watermark_topics: HashSet::new(),
            concrete_subscriptions: HashSet::new(),
            wild_subscriptions: Vec::new(),
            tracker_type: 0,
            active: true
        }
    }

    pub fn has_next(&self) -> bool {
        if !self.data_tracker.is_empty() || !self.watermarks_tracker.is_empty() || self.topics_tracker.is_some() {
            return true
        }

        false
    }

    pub fn add_subscription(&mut self, filter: &str) {
        if has_wildcards(filter) {
            self.wild_subscriptions.push(filter.to_owned());
            return;
        }

        self.concrete_subscriptions.insert(filter.to_owned());
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
        self.watermarks_tracker.push_back(request);
    }

    /// Match this topic to subscriptions this connection is interested in.
    /// Matches only if the topic isn't already tracked.
    fn match_and_track(&mut self, topic: String) {
        // ignore if the topic is already being tracked
        if self.data_topics.contains(&topic) {
            return;
        }

        // Adds a new topic to track. Duplicates are already covered by 'match_subscriptions'
        // A concrete subscription match. Add this new topic to data tracker
        if self.concrete_subscriptions.contains(&topic) {
            self.data_topics.insert(topic.clone());
            let request = DataRequest::new(topic);
            self.data_tracker.push_back(request);
            return;
        }

        for filter in self.wild_subscriptions.iter() {
            if matches(&topic, filter) {
                self.data_topics.insert(topic.clone());
                let request = DataRequest::new(topic);
                self.data_tracker.push_back(request);
                return;
            }
        }
    }

    pub fn update_watermarks_tracker(&mut self, reply: &AcksReply) {
        let request = AcksRequest::new(reply.topic.clone(), reply.offset);
        self.watermarks_tracker.push_back(request);
    }

    /// Updates data tracker to track more topics
    /// So, a TopicReply triggers DataRequest
    pub fn track_more_topics(&mut self, reply: &TopicsReply) {
        for topic in reply.topics.iter() {
            // Adds a DataRequest to data tracker if there is a match
            self.match_and_track(topic.clone());
        }

        self.topics_tracker = Some(TopicsRequest::offset(reply.offset + 1));
    }

    /// Updates offset of this topic for the next data request
    /// Note that we don't support removing a topic. Topics are only disabled when they
    /// are caught up. We don't remove even topics for unsubscribes at the moment.
    /// This allows for index of the tracker vector to not be invalidated when router
    /// holds a pending notification (be it immediate reply and notification when there
    /// is new data)
    pub fn update_data_tracker(&mut self, reply: &DataReply) {
        let request = DataRequest::offsets(
            reply.topic.clone(),
            reply.cursors
        );

        self.data_tracker.push_back(request);
    }

    /// Returns data request from next topic by cycling through all the tracks
    /// `next()` is only called by link only after it receives a reply of previous topic
    /// This ignores inactive indexes while iterating
    /// If all the tracks are pending, this returns `None` indicating that link should stop
    /// making any new requests to the router
    pub fn next(&mut self) -> Option<RouterInMessage> {
        let start_tracker = self.tracker_type;

        if self.tracker_type == 0 {
            // loop to go to next active topic if the topic at the current offset is inactive
            match self.data_tracker.pop_front() {
                Some(request) => {
                    let message = RouterInMessage::DataRequest(request);
                    return Some(message)
                }
                None => {
                    self.tracker_type = 1;
                }
            };
        }

        if self.tracker_type == 1 {
            match self.watermarks_tracker.pop_front() {
                Some(request) => {
                    let message = RouterInMessage::AcksRequest(request);
                    return Some(message)
                }
                None => {
                    // Reset the next position and track next tracker type
                    self.tracker_type = 2;
                }
            };
        }

        if self.tracker_type == 2 {
            self.tracker_type = 0;

            if let Some(request) = self.topics_tracker.take() {
                let message = RouterInMessage::TopicsRequest(request);
                return Some(message);
            }

            // if we fall from  top to bottom, all trackers are inactive
            if self.tracker_type == start_tracker {
                self.active = false;
                return None;
            }
        }

        None
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
