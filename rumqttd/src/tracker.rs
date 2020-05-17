use std::collections::HashSet;
use indexmap::IndexMap;
use rumqttlog::DataRequest;
use rumqttlog::mqtt4bytes::matches;

/// Tracker tracks current offsets of all the subscriptions of a link.
/// It also updates the iterator list to efficiently iterate over all
/// the topics without having to create an iterator and thus locking itself
/// to get updated. Instead tracker just returns number of topics it currently
/// host along with the status of the topic (active/inactive) for link
/// to use while iterating. This helps us prevent maintaining 2 lists
/// (active, inactive)
/// TODO HashMap iterations are not always very efficient. Check indexmap crate
pub struct Tracker {
    /// List of topics that we are tracking
    tracks: IndexMap<String, Track>,
    /// List of concrete subscriptions
    concrete_subscriptions: HashSet<String>,
    /// List of wildcard subscriptions
    wild_subscriptions: Vec<String>,
    /// Current iteration position of `tracks`
    next_position: usize
}

impl Tracker {
    pub fn new() -> Tracker {
        Tracker {
            tracks: IndexMap::new(),
            concrete_subscriptions: HashSet::new(),
            wild_subscriptions: vec!["#".to_owned()],
            next_position: 0
        }
    }

    /// Adds a new topic to track. Duplicates are already covered by 'match_subscriptions'
    fn add(&mut self, topic: String) {
        let track = Track::new(&topic);
        self.tracks.insert(topic, track);
    }

    /// Match the subscriptions this connection is interested in. Matches
    /// only if the topic isn't already tracked.
    pub fn match_subscription_and_add(&mut self, topic: String) {
        if self.tracks.contains_key(&topic) {
            return;
        }

        if self.concrete_subscriptions.contains(&topic) {
            self.add(topic);
            return;
        }

        for filter in self.wild_subscriptions.iter() {
            if matches(&topic, filter) {
                self.add(topic.clone());
                return;
            }
        }
    }

    /// Returns data request from next topic by cycling through all the tracks
    /// `next()` is only called by link only after it receives a reply of previous topic
    /// This ignores inactive indexes while iterating
    /// If all the tracks are pending, this returns `None` indicating that link should stop
    /// making any new requests to the router
    pub fn next(&mut self) -> Option<DataRequest> {
        let start_position = self.next_position;
        // FIXME We are iterating through all the topics which are inactive. This might be costly
        // when there are a lot of caught up topics (just like switching between active and inactive
        // topics is costly when the producer quickly produce more data). Maybe we can have a middle
        // ground where we move inactive topics to a different item when they are inactive for a few
        // iterations?
        loop {
            match self.tracks.get_index(self.next_position) {
                Some((_key, track)) => {
                    // Increment for next iteration of `next`
                    self.next_position += 1;
                    // Stop if next iteration is start again
                    // This implies all the topics are inactive
                    if !track.active && self.next_position == start_position { return None }
                    if !track.active { continue };
                    return Some(track.request.clone())
                },
                None => {
                    self.next_position = 0;
                    // Stop if next iteration is start again
                    // This implies all the topics are inactive
                    if self.next_position == start_position { return None }
                },
            }
        }
    }

    /// Updates offset of this topic for the next data request
    /// TODO Every time a topic catches up, we are removing it from active
    /// TODO and moving it to paused tracks so that next topics iterator doesn't
    /// TODO consider that. Probably there is a better way to to this with out
    /// TODO maintaining 2 tracks and moving between them? Probably save active/inactive
    /// TODO status in 'tracks' directly and use ids for iterating?
    pub fn update(&mut self, topic: &str, segment: u64, offset: u64, len: usize) {
        debug!(
            "Data reply. Topic = {}, segment = {}, offset = {}, len = {}",
            topic,
            segment,
            offset,
            len
        );

        // update topic request parameters for next data request
        // if the reply is not in active tracks, this is a woken up paused track. Add it
        // to active tracks to poll this in the next iteration
        if let Some(track) = self.tracks.get_mut(topic) {
            track.request.segment = segment;
            track.request.offset = offset + 1;

            // Mark the caught up topic as inactive to allow link iterator to ignore this
            if len == 0 {
                // debug!("Caught up subscription. Topic = {:?}", topic);
                track.active = false;
            } else {
                // debug!("Reactivating a subscription. Topic = {:?}", topic);
                track.active = true;
            }

            track.acked = true;
        }
    }
}

#[derive(Debug)]
pub struct Track {
    /// Next data request
    request: DataRequest,
    /// Topic active or caught up
    active: bool,
    /// Last data request acked or pending
    acked: bool
}

impl Track {
    pub fn new(topic: &str) -> Track {
        let request = DataRequest {
            topic: topic.to_string(),
            segment: 0,
            offset: 0,
            size: 1024 * 1024,
        };

        Track {
            request,
            active: true,
            acked: true
        }
    }
}

// TODO: Replace all `new()` with `with_capacity()`

#[cfg(test)]
mod test {
    use super::Tracker;

    #[test]
    fn next_track_iterates_through_tracks_correctly() {
        let mut tracker = Tracker::new();
        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.add(topic);
        }

        for i in 0..100 {
            let topic = tracker.next().unwrap().topic;
            let expected = format!("{}", i % 10);
            assert_eq!(topic, expected);
        }

        // now add a new topics to the tracker
        for i in 10..20 {
            let topic = format!("{}", i);
            tracker.add(topic);
        }


        for i in 10..100 {
            let topic = tracker.next().unwrap().topic;
            let expected = format!("{}", i % 20);
            assert_eq!(topic, expected);
        }
    }

    #[test]
    fn next_track_does_not_iterate_through_inactive_track() {
        let mut tracker = Tracker::new();
        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.add(topic);
        }

        // disable alternate topics
        for i in 0..10 {
            if i % 2 == 0 {
                let topic = format!("{}", i);
                tracker.update(&topic, 0, 10, 0);
            }
        }


        for i in (1..100).step_by(2) {
            let topic = tracker.next().unwrap().topic;
            let expected = format!("{}", i % 10);
            assert_eq!(topic, expected);
        }
    }

    #[test]
    fn reactivated_topic_is_included_in_next_iteration() {
        let mut tracker = Tracker::new();
        tracker.add("1".to_owned());
        tracker.add("2".to_owned());

        // deactivate a topic
        tracker.update("1", 0, 10, 0);
        assert_eq!(tracker.next().unwrap().topic, "2");
        assert_eq!(tracker.next().unwrap().topic, "2");
        assert_eq!(tracker.next().unwrap().topic, "2");
        assert_eq!(tracker.next().unwrap().topic, "2");

        // reactivate it again
        tracker.update("1", 0, 10, 1);
        assert_eq!(tracker.next().unwrap().topic, "1");
        assert_eq!(tracker.next().unwrap().topic, "2");
        assert_eq!(tracker.next().unwrap().topic, "1");
        assert_eq!(tracker.next().unwrap().topic, "2");
    }

    #[test]
    fn all_inactive_topics_should_return_none() {
        let mut tracker = Tracker::new();
        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.add(topic);
        }

        // disable all the topics
        for i in 0..10 {
            let topic = format!("{}", i);
            tracker.update(&topic, 0, 10, 0);
        }

        assert!(tracker.next().is_none());
    }

    #[test]
    fn tracks_return_none_only_when_current_topic_is_not_active() {
        let mut tracker = Tracker::new();
        tracker.add("1".to_owned());
        assert!(tracker.next().is_some());
    }
}
