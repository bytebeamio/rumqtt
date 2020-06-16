use std::collections::HashSet;

use crate::{DataRequest, RouterInMessage, DataReply};
use mqtt4bytes::{has_wildcards, matches};
use crate::router::{TopicsRequest, WatermarksRequest, WatermarksReply, TopicsReply};

/// Tracker tracks current offsets of all the subscriptions of a connection
/// It also updates the iterator list to efficiently iterate over all
/// the topics without having to create an iterator and thus locking itself
/// to get updated. Instead tracker just returns number of topics it currently
/// host along with the status of the topic (active/inactive) for link
/// to use while iterating. This helps us prevent maintaining 2 lists
/// (active, inactive)
pub struct Tracker {
    /// List of topics that we are tracking for data
    data_tracker: Vec<DataTracker>,
    /// List of topics that we are tracking for watermark
    watermarks_tracker: Vec<WatermarksTracker>,
    /// List of topics that we are tracking for watermark
    topics_tracker: TopicsTracker,
    /// List of topics that are being tracked for data. This
    /// index is used to not add topics which are are already
    /// being tracked by `data_requests`
    data_topics: HashSet<String>,
    watermark_topics: HashSet<String>,
    /// List of concrete subscriptions
    concrete_subscriptions: HashSet<String>,
    /// List of wildcard subscriptions
    wild_subscriptions: Vec<String>,
    /// Current tracker type. Data = 0, Watermarks = 1
    tracker_type: u8,
    /// Current iteration position of `tracks`
    next_index: usize,
}

impl Tracker {
    pub fn new() -> Tracker {
        Tracker {
            data_tracker: Vec::new(),
            watermarks_tracker: Vec::new(),
            topics_tracker: TopicsTracker::new(),
            data_topics: HashSet::new(),
            watermark_topics: HashSet::new(),
            concrete_subscriptions: HashSet::new(),
            wild_subscriptions: Vec::new(),
            tracker_type: 0,
            next_index: 0,
        }
    }

    pub fn add_subscription(&mut self, filter: &str) {
        if has_wildcards(filter) {
            self.wild_subscriptions.push(filter.to_owned());
            return;
        }

        self.concrete_subscriptions.insert(filter.to_owned());
    }

    /// Adds a new topic to watermarks tracker
    pub fn track_watermark(&mut self, topic: String) {
        // ignore if the topic is already being tracked
        if self.watermark_topics.contains(&topic) {
            return;
        }

        self.watermark_topics.insert(topic.to_owned());
        let request = WatermarksTracker::new(topic, self.watermarks_tracker.len());
        self.watermarks_tracker.push(request);
    }

    /// Match the subscriptions this connection is interested in. Matches
    /// only if the topic isn't already tracked.
    pub fn track_matched_topics(&mut self, topic: String) {
        // ignore if the topic is already being tracked
        if self.data_topics.contains(&topic) {
            return;
        }

        // Adds a new topic to track. Duplicates are already covered by 'match_subscriptions'
        // A concrete subscription match. Add this new topic to data tracker
        if self.concrete_subscriptions.contains(&topic) {
            self.data_topics.insert(topic.clone());
            let request = DataTracker::new(topic, self.data_tracker.len());
            self.data_tracker.push(request);
            return;
        }

        for filter in self.wild_subscriptions.iter() {
            if matches(&topic, filter) {
                self.data_topics.insert(topic.clone());
                let request = DataTracker::new(topic, self.data_tracker.len());
                self.data_tracker.push(request);
                return;
            }
        }
    }

    pub fn update_watermarks_request(&mut self, reply: &WatermarksReply) {
        match self.watermarks_tracker.get_mut(reply.tracker_topic_offset) {
            Some(track) => {
                let caughtup = reply.watermarks.is_empty();
                if caughtup {
                    track.active = false;
                    return
                }

                track.active = true
            }
            None => panic!("Invalid index while accessing data tracker"),
        }
    }

    pub fn update_topics_request(&mut self, reply: &TopicsReply) {
        let caughtup = reply.topics.is_empty();
        if caughtup {
            self.topics_tracker.active = false;
            return;
        }

        self.topics_tracker.active = true;
        self.topics_tracker.request.offset = reply.offset + 1;
    }

    /// Updates offset of this topic for the next data request
    /// Note that we don't support removing a topic. Topics are only disabled when they
    /// are caught up. We don't remove even topics for unsubscribes at the moment.
    /// This allows for index of the tracker vector to not be invalidated when router
    /// holds a pending notification (be it immediate reply and notification when there
    /// is new data)
    pub fn update_data_request(&mut self, reply: &DataReply) {
        match self.data_tracker.get_mut(reply.tracker_topic_offset) {
            Some(track) => {
                let native_topic_caughtup = reply.native_count == 0;
                let replicated_topic_caughtup = reply.replica_count == 0;

                if native_topic_caughtup && replicated_topic_caughtup {
                    track.active = false;
                    return
                }

                if !native_topic_caughtup {
                    track.request.native_segment = reply.native_segment;
                    track.request.native_offset = reply.native_offset + 1;
                }

                if !replicated_topic_caughtup {
                    track.request.replica_segment = reply.replica_segment;
                    track.request.replica_offset = reply.replica_offset + 1;
                }

                track.active = true;
            }
            None => panic!("Invalid index while accessing data tracker"),
        }
    }

    /// Returns data request from next topic by cycling through all the tracks
    /// `next()` is only called by link only after it receives a reply of previous topic
    /// This ignores inactive indexes while iterating
    /// If all the tracks are pending, this returns `None` indicating that link should stop
    /// making any new requests to the router
    pub fn next(&mut self) -> Option<RouterInMessage> {
        let start_index = self.next_index;
        let start_tracker = self.tracker_type;

        // We are iterating through all the topics which are inactive. This might be costly
        // when there are a lot of caught up topics (just like switching between active and inactive
        // topics is costly when the producer quickly produce more data). Maybe we can have a middle
        // ground where we move inactive topics to a different item when they are inactive for a few
        // iterations?
        loop {
            if self.tracker_type == 0 {
                // loop to go to next active topic if the topic at the current offset is inactive
                match self.data_tracker.get(self.next_index) {
                    Some(track) => {
                        // Increment for next iteration of `next`
                        self.next_index += 1;
                        if !track.active {
                            continue;
                        };

                        let message = RouterInMessage::DataRequest(track.request.clone());
                        return Some(message);
                    }
                    None => {
                        // Reset the next position and track next tracker type
                        self.next_index = 0;
                        self.tracker_type = 1;

                        // Stop if next iteration is start again. All the topics are inactive
                        if self.next_index == start_index && self.tracker_type == start_tracker {
                            return None;
                        }
                    }
                }
            }

            if self.tracker_type == 1 {
                match self.watermarks_tracker.get(self.next_index) {
                    Some(track) => {
                        // Increment for next iteration of `next`
                        self.next_index += 1;

                        // Stop if next iteration is start again. All the topics are inactive
                        if !track.active && self.next_index == start_index {
                            return None;
                        }
                        if !track.active {
                            continue;
                        };
                        let message = RouterInMessage::WatermarksRequest(track.request.clone());
                        return Some(message);
                    }
                    None => {
                        // Reset the next position and track next tracker type
                        self.tracker_type = 2;

                        // Stop if next iteration is start again. All the topics are inactive
                        if self.next_index == start_index && self.tracker_type == start_tracker {
                            return None;
                        };
                    }
                }
            }

            if self.tracker_type == 2 {
                self.tracker_type = 0;
                self.next_index = 0;

                // Stop if next iteration is start again. All the topics are inactive
                if !self.topics_tracker.active && self.next_index == start_index {
                    return None;
                }
                if !self.topics_tracker.active {
                    continue;
                };

                let message = RouterInMessage::TopicsRequest(self.topics_tracker.request.clone());
                return Some(message);
            }
        }
    }
}

#[derive(Debug)]
pub struct DataTracker {
    /// Next data request
    request: DataRequest,
    /// Topic active or caught up
    active: bool,
}

impl DataTracker {
    pub fn new(topic: String, tracker_topic_offset: usize) -> DataTracker {
        let request = DataRequest {
            topic,
            native_segment: 0,
            replica_segment: 0,
            native_offset: 0,
            replica_offset: 0,
            tracker_topic_offset,
            size: 1024 * 1024,
        };

        DataTracker {
            request,
            active: true,
        }
    }
}

#[derive(Debug)]
pub struct TopicsTracker {
    request: TopicsRequest,
    active: bool,
}

impl TopicsTracker {
    pub fn new() -> TopicsTracker {
        let request = TopicsRequest {
            offset: 0,
            count: 100,
        };

        TopicsTracker {
            request,
            active: true,
        }
    }
}

#[derive(Debug)]
pub struct WatermarksTracker {
    request: WatermarksRequest,
    active: bool,
}

impl WatermarksTracker {
    pub fn new(topic: String, tracker_topic_offset: usize) -> WatermarksTracker {
        let request = WatermarksRequest {
            topic,
            watermarks: vec![],
            tracker_topic_offset,
        };

        WatermarksTracker {
            request,
            active: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{DataReply, RouterInMessage};
    use bytes::Bytes;
    use crate::router::{WatermarksReply, TopicsReply};
    use crate::tracker::Tracker;

    #[test]
    fn next_track_iterates_through_tracks_correctly() {
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");

        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.track_matched_topics(topic.clone());
            tracker.track_watermark(topic);
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
            tracker.track_matched_topics(topic.clone());
            tracker.track_watermark(topic);
        }

        // 20 data requests
        for i in 0..20 {
            let topic = get_data_topic(&tracker.next().unwrap());
            let expected = format!("{}", i);
            assert_eq!(topic, expected);
        }

        for i in 0..20 {
            let topic = get_watermarks_topic(&tracker.next().unwrap());
            let expected = format!("{}", i);
            assert_eq!(topic, expected);
        }
    }

    #[test]
    fn next_track_does_not_iterate_through_inactive_track() {
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");

        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.track_matched_topics(topic.clone());
            tracker.track_watermark(topic);
        }

        // 10 data requests. Disable all even topics
        for i in 0..10 {
            let request = tracker.next().unwrap();
            if i % 2 == 0 {
                let message = empty_data_reply(request);
                tracker.update_data_request(&message);
            }
        }

        // reset the tracker
        tracker.next_index = 0;
        for i in (1..10).step_by(2) {
            let topic = get_data_topic(&tracker.next().unwrap());
            let expected = format!("{}", i);
            assert_eq!(topic, expected);
        }
    }

    #[test]
    fn reactivated_topic_is_included_in_next_iteration() {
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");

        // create topics in all disabled state
        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.track_matched_topics(topic.clone());
            tracker.track_watermark(topic);
        }

        // disable all topics in data tracker
        for _ in 0..10 {
            let request = tracker.next().unwrap();
            let message = empty_data_reply(request);
            tracker.update_data_request(&message);
        }

        // disable all topics in watermark tracker
        for _ in 0..10 {
            let request = tracker.next().unwrap();
            let message = empty_watermarks_reply(request);
            tracker.update_watermarks_request(&message);
        }

        // disable topics tracker
        let _request = tracker.next().unwrap();
        let message = empty_topics_reply();
        tracker.update_topics_request(&message);

        // no topics to track
        assert!(tracker.next().is_none());

        // re-enable all data topics
        for i in 0..10 {
            let topic = format!("{}", i);
            let message = filled_data_reply(&topic, i);
            tracker.update_data_request(&message);
        }

        // reset the tracker position. next() should read from data track again
        tracker.next_index = 0;
        tracker.tracker_type = 0;
        for i in 0..10 {
            let topic = get_data_topic(&tracker.next().unwrap());
            assert_eq!(topic, format!("{}", i));
        }

        // re-enable all watermark topics
        for i in 0..10 {
            let topic = format!("{}", i);
            let message = filled_watermarks_reply(&topic, i);
            tracker.update_watermarks_request(&message);
        }

        // tracker now iterates through watermarks
        for i in 0..10 {
            let topic = get_watermarks_topic(&tracker.next().unwrap());
            assert_eq!(topic, format!("{}", i));
        }

        let message = filled_topics_reply();
        tracker.update_topics_request(&message);

        // next() returns topics request now
        let message = tracker.next().unwrap();
        assert!(is_topics_request(&message));
    }

    #[test]
    fn all_inactive_topics_should_return_none() {
        let mut tracker = Tracker::new();
        tracker.add_subscription("#");

        let message = tracker.next().unwrap();
        assert!(is_topics_request(&message));

        // disable topics request with empty reply
        let reply = empty_topics_reply();
        tracker.update_topics_request(&reply);

        // no topics to track
        assert!(tracker.next().is_none());
    }

    fn empty_data_reply(request: RouterInMessage) -> DataReply {
        let request = match request {
            RouterInMessage::DataRequest(r) => r,
            request => panic!("Expecting data request. Received = {:?}", request),
        };

        let reply = DataReply {
            done: true,
            topic: request.topic,
            native_segment: 0,
            native_offset: 0,
            native_count: 0,
            replica_segment: 0,
            replica_offset: 0,
            replica_count: 0,
            pkids: vec![],
            tracker_topic_offset: request.tracker_topic_offset,
            payload: vec![],
        };

        reply
    }

    fn empty_watermarks_reply(request: RouterInMessage) -> WatermarksReply {
        let request = match request {
            RouterInMessage::WatermarksRequest(r) => r,
            request => panic!("Expecting watermarks request. Received = {:?}", request),
        };

        let reply = WatermarksReply {
            topic: request.topic,
            watermarks: vec![],
            tracker_topic_offset: request.tracker_topic_offset,
        };

        reply
    }

    fn empty_topics_reply() -> TopicsReply {
        let reply = TopicsReply {
            offset: 0,
            topics: vec![],
        };

        reply
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
            tracker_topic_offset,
            payload: vec![Bytes::from(vec![1, 2, 3])],
        };

        reply
    }

    fn filled_watermarks_reply(topic: &str, tracker_topic_offset: usize) -> WatermarksReply {
        let reply = WatermarksReply {
            topic: topic.to_owned(),
            watermarks: vec![1, 2, 3],
            tracker_topic_offset
        };

        reply
    }

    fn filled_topics_reply() -> TopicsReply {
        let reply = TopicsReply {
            offset: 1,
            topics: vec!["hello/world".to_owned()]
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
            RouterInMessage::WatermarksRequest(request) => request.topic.clone(),
            v => panic!("Expecting data request. Received = {:?}", v),
        }
    }

    fn is_topics_request(message: &RouterInMessage) -> bool {
        match message {
            RouterInMessage::TopicsRequest(_) => true,
            v => panic!("Expecting data request. Received = {:?}", v),
        }
    }
}
