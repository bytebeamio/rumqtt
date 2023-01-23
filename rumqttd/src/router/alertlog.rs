use slab::Slab;

use crate::protocol::{matches, Subscribe, Unsubscribe};
use crate::router::FilterIdx;
use crate::{Filter, Offset, RouterConfig, Topic};

use crate::segments::{CommitLog, Position};
use crate::Storage;
use std::collections::HashMap;
use std::io;

#[derive(Debug, Clone)]
pub enum AlertError {
    CursorJump(String),
}

impl AlertError {
    pub fn topic(&self) -> String {
        match self {
            AlertError::CursorJump(_) => "/error".to_string(),
        }
    }
}

impl Storage for AlertError {
    fn size(&self) -> usize {
        match self {
            Self::CursorJump(message) => message.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum AlertEvent {
    Connect,
    Disconnect,
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
}

impl AlertEvent {
    pub fn topic(&self) -> String {
        match self {
            Self::Connect => "/event/connect".to_string(),
            Self::Disconnect => "/event/disconnect".to_string(),
            Self::Subscribe(_) => "/event/subscribe".to_string(),
            Self::Unsubscribe(_) => "/event/unsubscribe".to_string(),
        }
    }
}

impl Storage for AlertEvent {
    fn size(&self) -> usize {
        match self {
            Self::Connect => 0,
            Self::Disconnect => 0,
            Self::Subscribe(subscribe) => {
                let mut size = 0;
                size += 2;
                for filter in &subscribe.filters {
                    size += filter.path.len();
                }
                size
            }
            Self::Unsubscribe(unsubscribe) => {
                let mut size = 0;
                size += 2;
                for filter in &unsubscribe.filters {
                    size += filter.len();
                }
                size
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Alert {
    Event(String, AlertEvent),
    Error(String, AlertError),
}

impl Alert {
    pub fn event(client_id: String, event: AlertEvent) -> Self {
        Alert::Event(client_id, event)
    }

    pub fn error(client_id: String, error: AlertError) -> Self {
        Alert::Error(client_id, error)
    }
}

impl Storage for Alert {
    fn size(&self) -> usize {
        match self {
            Alert::Event(client_id, event) => client_id.len() + event.size(),
            Alert::Error(client_id, alert) => client_id.len() + alert.size(),
        }
    }
}

impl Alert {
    pub fn topic(&self) -> String {
        match self {
            Alert::Event(client_id, event) => format!("/alerts{}/{client_id}", event.topic()),
            Alert::Error(client_id, error) => format!("/alerts{}/{client_id}", error.topic()),
        }
    }
}

pub struct AlertLog {
    pub config: RouterConfig,
    /// Native commitlog data organized by subscription. Contains
    /// device data and actions data logs.
    ///
    /// Device data is replicated while actions data is not.
    /// Also has waiters used to wake connections/replicator tracker
    /// which are caught up with all the data on 'Filter' and waiting
    /// for new data
    pub native: Slab<AlertData<Alert>>,
    /// Map of subscription filter name to filter index
    filter_indexes: HashMap<Filter, FilterIdx>,
    /// List of filters associated with a topic
    publish_filters: HashMap<Topic, Vec<FilterIdx>>,
}

impl AlertLog {
    pub fn new(config: RouterConfig) -> io::Result<AlertLog> {
        let native = Slab::new();
        let filter_indexes = HashMap::new();
        let publish_filters = HashMap::new();

        Ok(AlertLog {
            config,
            native,
            publish_filters,
            filter_indexes,
        })
    }
    // TODO: Currently returning a Option<Vec> instead of Option<&Vec> due to Rust borrow checker
    // limitation
    pub fn matches(&mut self, topic: &str) -> Option<Vec<usize>> {
        match &self.publish_filters.get(topic) {
            Some(v) => Some(v.to_vec()),
            None => {
                let v: Vec<usize> = self
                    .filter_indexes
                    .iter()
                    .filter(|(filter, _)| matches(topic, filter))
                    .map(|(_, filter_idx)| *filter_idx)
                    .collect();

                if !v.is_empty() {
                    self.publish_filters.insert(topic.to_owned(), v.clone());
                }

                Some(v)
            }
        }
    }

    pub fn next_native_offset(&mut self, filter: &str) -> (FilterIdx, Offset) {
        let publish_filters = &mut self.publish_filters;
        let filter_indexes = &mut self.filter_indexes;

        let (filter_idx, data) = match filter_indexes.get(filter) {
            Some(idx) => (*idx, self.native.get(*idx).unwrap()),
            None => {
                let data = AlertData::new(
                    filter,
                    self.config.max_segment_size,
                    self.config.max_segment_count,
                );

                // Add commitlog to datalog and add datalog index to filter to
                // datalog index map
                let idx = self.native.insert(data);
                self.filter_indexes.insert(filter.to_owned(), idx);

                // Match new filter to existing topics and add to publish_filters if it matches
                for (topic, filters) in publish_filters.iter_mut() {
                    if matches(topic, filter) {
                        filters.push(idx);
                    }
                }

                (idx, self.native.get(idx).unwrap())
            }
        };

        (filter_idx, data.log.next_offset())
    }

    pub fn native_readv(
        &mut self,
        filter: Filter,
        offset: Offset,
        len: u64,
    ) -> io::Result<(Vec<(Alert, Offset)>, Offset)> {
        let filter_idx = *self.filter_indexes.get(&filter).unwrap();
        let data = self.native.get(filter_idx).unwrap();
        let mut o = Vec::new();

        let next = data.log.readv(offset, len, &mut o)?;
        let next_offset = match next {
            Position::Next { start: _, end } => end,
            Position::Done { start: _, end } => end,
        };
        Ok((o, next_offset))
    }
}

pub struct AlertData<T> {
    filter: Filter,
    pub log: CommitLog<T>,
}

impl<T> AlertData<T>
where
    T: Storage + Clone,
{
    pub fn new(filter: &str, max_segment_size: usize, max_mem_segments: usize) -> AlertData<T> {
        let log = CommitLog::new(max_segment_size, max_mem_segments).unwrap();
        AlertData {
            filter: filter.to_owned(),
            log,
        }
    }

    /// Writes to all the filters that are mapped to this publish topic
    /// and wakes up consumers that are matching this topic (if they exist)
    pub fn append(&mut self, item: T) -> (Offset, &Filter) {
        let offset = self.log.append(item);
        (offset, &self.filter)
    }
}
