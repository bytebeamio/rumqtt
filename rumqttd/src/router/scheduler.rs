use std::collections::{HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use slab::Slab;
use tracing::trace;

use super::DataRequest;
use crate::{ConnectionId, Filter};

pub struct Scheduler {
    /// Subscriptions and matching topics maintained per connection
    pub trackers: Slab<Tracker>,
    /// Connections with more pending requests and ready to make progress
    pub readyqueue: VecDeque<ConnectionId>,
}

impl Scheduler {
    pub fn with_capacity(capacity: usize) -> Scheduler {
        Scheduler {
            trackers: Slab::with_capacity(capacity),
            readyqueue: VecDeque::with_capacity(capacity),
        }
    }

    pub fn add(&mut self, tracker: Tracker) -> ConnectionId {
        self.trackers.insert(tracker)
    }

    pub fn remove(&mut self, id: ConnectionId) -> Tracker {
        self.trackers.remove(id)
    }

    /// Next connection which is ready to make progress
    pub fn poll(&mut self) -> Option<(ConnectionId, VecDeque<DataRequest>)> {
        let id = self.readyqueue.pop_front()?;
        let tracker = self.trackers.get_mut(id)?;

        // drain will clear all DataRequest but will keep the allocated memory of our VecDeque.
        let data_requests = tracker.data_requests.drain(..).collect();

        // Implicitly reschedule the connection. Router will take care of explicitly pausing if
        // required (it has the state necessary to determine if pausing is required)
        self.readyqueue.push_back(id);
        Some((id, data_requests))
    }

    pub fn track(&mut self, id: ConnectionId, request: DataRequest) {
        let tracker = self.trackers.get_mut(id).unwrap();
        tracker.register_data_request(request);
    }

    pub fn untrack(&mut self, id: ConnectionId, filter: &Filter) {
        let tracker = self.trackers.get_mut(id).unwrap();
        tracker.unregister_data_request(filter.clone());
    }

    pub fn trackv(&mut self, id: ConnectionId, requests: VecDeque<DataRequest>) {
        let tracker = self.trackers.get_mut(id).unwrap();
        tracker.data_requests.extend(requests);
    }

    pub fn reschedule(&mut self, id: ConnectionId, reason: ScheduleReason) {
        let tracker = self.trackers.get_mut(id).unwrap();
        if let Some(v) = tracker.try_ready(reason) {
            trace!(tracker_id = tracker.id, "reschedule {:?} -> Ready", v);
            self.readyqueue.push_back(id);
        }
    }

    pub fn pause(&mut self, id: ConnectionId, reason: PauseReason) {
        assert_eq!(self.readyqueue.pop_back(), Some(id));
        let tracker = self.trackers.get_mut(id).unwrap();

        trace!(
            tracker_id = tracker.id,
            "pause {:?} -> {:?}",
            tracker.status,
            reason
        );
        tracker.pause(reason);
    }
}

/// Used to register a new connection with the router
/// Connection messages encompasses a handle for router to
/// communicate with this connection
/// TODO: Don't expose tracker to the outside world
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tracker {
    pub id: String,
    /// Data requests of all the subscriptions of this connection
    pub data_requests: VecDeque<DataRequest>,
    /// State machine
    pub status: Status,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Ready,
    Paused(PauseReason),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduleReason {
    Init,
    NewFilter,
    FreshData,
    IncomingAck,
    Ready,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PauseReason {
    Caughtup,
    InflightFull,
    Busy,
}

impl Tracker {
    pub fn new(client_id: String) -> Tracker {
        let requests = VecDeque::with_capacity(2);
        Tracker {
            id: client_id,
            data_requests: requests,
            status: Status::Paused(PauseReason::Busy),
        }
    }

    pub fn _reset(&mut self) {
        self.data_requests.clear();
        self.status = Status::Paused(PauseReason::Busy);
    }

    /// Sets the state to ready if correct event to bring out of paused state
    /// is received.
    ///
    /// Doesn't change the state to ready if counter event is not received or
    /// if already in ready state
    ///
    /// Returns Some(previous state) if status is changed to ready.
    /// None return implies either the connection is already ready of the
    /// state cannot be changed to ready
    pub fn try_ready(&mut self, reason: ScheduleReason) -> Option<PauseReason> {
        let previous = match self.status {
            Status::Ready => return None,
            Status::Paused(p) => p,
        };

        match reason {
            ScheduleReason::Init => {
                debug_assert!(self.status == Status::Paused(PauseReason::Busy));
                self.status = Status::Ready;
                Some(previous)
            }
            ScheduleReason::NewFilter if previous == PauseReason::Caughtup => {
                self.status = Status::Ready;
                Some(previous)
            }
            ScheduleReason::FreshData if previous == PauseReason::Caughtup => {
                self.status = Status::Ready;
                Some(previous)
            }
            ScheduleReason::IncomingAck if previous == PauseReason::InflightFull => {
                self.status = Status::Ready;
                Some(previous)
            }
            ScheduleReason::Ready => {
                debug_assert!(self.status == Status::Paused(PauseReason::Busy));
                self.status = Status::Ready;
                Some(previous)
            }
            _ => None,
        }
    }

    pub fn pause(&mut self, reason: PauseReason) {
        self.status = Status::Paused(reason);
    }

    pub fn _is_empty(&self) -> bool {
        self.data_requests.is_empty()
    }

    pub fn get_data_requests(&self) -> &VecDeque<DataRequest> {
        &self.data_requests
    }

    pub fn register_data_request(&mut self, request: DataRequest) {
        self.data_requests.push_back(request);
    }

    pub fn unregister_data_request(&mut self, filter: Filter) {
        self.data_requests
            .retain(|data_req| data_req.filter != filter);
    }
}

// Methods to check duplicates in trackers and schedulers
impl Scheduler {
    // Return a `Some` if duplicate were found, otherwise `None`
    pub fn check_readyqueue_duplicates(&self) -> Option<&VecDeque<ConnectionId>> {
        let readyqueue = &self.readyqueue;
        // In _worst_ case where all elements are unique, the size of uniq will be same as len of readyqueue
        let mut uniq = HashSet::with_capacity(readyqueue.len());

        let all_uniq = readyqueue.iter().all(|x| uniq.insert(x));

        if !all_uniq {
            Some(readyqueue)
        } else {
            None
        }
    }

    // Return a `Some` if duplicate were found, otherwise `None`
    pub fn check_tracker_duplicates(&self, id: ConnectionId) -> Option<&VecDeque<DataRequest>> {
        let tracker = self.trackers.get(id).unwrap();
        let tracker_data_req = tracker.get_data_requests();
        let mut uniq = HashSet::with_capacity(tracker_data_req.len());

        let all_uniq = tracker_data_req.iter().all(|x| uniq.insert(x.filter_idx));

        if !all_uniq {
            Some(&tracker.data_requests)
        } else {
            None
        }
    }
}
