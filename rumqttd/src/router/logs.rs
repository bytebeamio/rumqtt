use super::Ack;
use parking_lot::Mutex;
use slab::Slab;
use tracing::trace;

use crate::protocol::{
    matches, ConnAck, PingResp, PubAck, PubComp, PubRec, PubRel, Publish, SubAck, UnsubAck,
};
use crate::router::{DataRequest, FilterIdx, SubscriptionMeter, Waiters};
use crate::{ConnectionId, Filter, Offset, RouterConfig, Topic};

use crate::segments::{CommitLog, Position};
use crate::Storage;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;

/// Stores 'device' data and 'actions' data in native commitlog
/// organized by subscription filter. Device data is replicated
/// while actions data is not
pub struct DataLog {
    pub config: RouterConfig,
    /// Native commitlog data organized by subscription. Contains
    /// device data and actions data logs.
    ///
    /// Device data is replicated while actions data is not.
    /// Also has waiters used to wake connections/replicator tracker
    /// which are caught up with all the data on 'Filter' and waiting
    /// for new data
    pub native: Slab<Data<Publish>>,
    /// Map of subscription filter name to filter index
    filter_indexes: HashMap<Filter, FilterIdx>,
    retained_publishes: HashMap<Topic, Publish>,
    /// List of filters associated with a topic
    publish_filters: HashMap<Topic, Vec<FilterIdx>>,
    pub read_state: Arc<Mutex<ReadState>>,
}

#[derive(Default, Debug)]
pub struct PublishState {
    publish_offsets: Vec<Vec<Option<Offset>>>,
    filter_to_row_id: HashMap<FilterIdx, usize>,
    pubacks: Vec<PubAck>,
    read_marker_column: Option<usize>,
}

#[derive(Default, Debug)]
pub struct WriteState {
    inner: HashMap<Topic, PublishState>,
}

impl WriteState {
    pub fn register_write(
        &mut self,
        topic: Topic,
        puback: PubAck,
        publish_offsets: HashMap<FilterIdx, Offset>,
    ) {
        // find publish state for the topic
        let publish_state = self.inner.entry(topic).or_default();

        // update publish state
        publish_state.update(puback, publish_offsets);
    }
}

#[derive(Default, Debug)]
struct ReadMarker {
    subscriber_markers: HashMap<ConnectionId, Offset>,
    slowest_marker: Option<Offset>,
}

impl ReadMarker {
    // Return true if slowest_marker moved ahead
    pub fn update_subscriber_marker(
        &mut self,
        subscriber_id: ConnectionId,
        marker: Offset,
    ) -> bool {
        // after this operations the slowest_marker >= pre operations slowest_marker
        *self.subscriber_markers.entry(subscriber_id).or_default() = marker;
        self.compute_slowest_marker()
    }

    // Return true if slowest_marker moved ahead
    fn compute_slowest_marker(&mut self) -> bool {
        let prev_slowest_marker = self.slowest_marker;
        self.slowest_marker = self.subscriber_markers.values().min().copied();

        self.slowest_marker > prev_slowest_marker
    }

    pub fn get_slowest_marker(&self) -> Option<Offset> {
        self.slowest_marker
    }
}

// give read state to the commit log
// data log -> readstate
#[derive(Default, Debug)]
pub struct ReadState {
    inner: HashMap<FilterIdx, Offset>,
    read_markers: HashMap<FilterIdx, ReadMarker>,
    filter_publishers: HashMap<FilterIdx, Vec<ConnectionId>>,
}

impl ReadState {
    pub fn register_read(
        &mut self,
        filter_idx: FilterIdx,
        connection_id: ConnectionId,
        last_persisted_read_offset: Offset,
    ) -> Option<Offset> {
        let read_marker = self.read_markers.entry(filter_idx).or_default();

        let read_progress =
            read_marker.update_subscriber_marker(connection_id, last_persisted_read_offset);

        if read_progress {
            *self.inner.entry(filter_idx).or_default() = read_marker.get_slowest_marker().unwrap();

            read_marker.get_slowest_marker()
        } else {
            None
        }
    }

    pub fn register_publisher(&mut self, publisher_id: ConnectionId, filter_id: FilterIdx) {
        self.filter_publishers
            .entry(filter_id)
            .or_default()
            .push(publisher_id);
    }

    pub fn get_publishers_for_filter(&self, filter_idx: FilterIdx) -> &Vec<ConnectionId> {
        self.filter_publishers.get(&filter_idx).unwrap()
    }
}

#[derive(Debug)]
pub struct Acker {
    read_state: Arc<Mutex<ReadState>>,
    write_state: WriteState,
}

impl Acker {
    pub fn new(read_state: Arc<Mutex<ReadState>>) -> Acker {
        Acker {
            read_state,
            write_state: Default::default(),
        }
    }

    pub fn register_write(
        &mut self,
        topic: Topic,
        puback: PubAck,
        publish_offsets: HashMap<FilterIdx, Offset>,
    ) {
        self.write_state
            .register_write(topic, puback, publish_offsets);
    }

    pub fn release_acks(&mut self) -> Vec<PubAck> {
        // What about out of order acks? A_3, A_0, A_1, A_4, A_2, A_5, A_6, ...?

        let progress = self.sync();
        let mut acks_to_be_released = Vec::new();

        for (topic, (prev_marker, curr_marker)) in progress {
            let write_state = self.write_state.inner.get_mut(&topic).unwrap();

            if let Some(curr) = curr_marker {
                let prev = match prev_marker {
                    Some(prev) => prev + 1,
                    None => 0,
                };
                let mut queued_acks = write_state.pubacks[prev..=curr].to_vec();

                acks_to_be_released.append(&mut queued_acks);
            }
        }

        acks_to_be_released
    }

    fn sync(&mut self) -> HashMap<Topic, (Option<usize>, Option<usize>)> {
        let current_read_state = &self.read_state.lock().inner;

        let mut progress = HashMap::<Topic, (Option<usize>, Option<usize>)>::new();
        for (topic, publish_state) in self.write_state.inner.iter_mut() {
            let prev_marker = publish_state.read_marker_column;
            publish_state.recompute_read_marker(current_read_state);
            let curr_marker = publish_state.read_marker_column;

            if curr_marker > prev_marker {
                progress.insert(topic.to_owned(), (prev_marker, curr_marker));
            }
        }

        println!("progress is {progress:?}");
        progress
    }
}

impl DataLog {
    pub fn new(config: RouterConfig) -> io::Result<DataLog> {
        let mut native = Slab::new();
        let mut filter_indexes = HashMap::new();
        let retained_publishes = HashMap::new();
        let publish_filters = HashMap::new();

        if let Some(warmup_filters) = config.initialized_filters.clone() {
            for filter in warmup_filters {
                let data = Data::new(&filter, config.max_segment_size, config.max_segment_count);

                // Add commitlog to datalog and add datalog index to filter to
                // datalog index map
                let idx = native.insert(data);
                filter_indexes.insert(filter, idx);
            }
        }

        Ok(DataLog {
            config,
            native,
            publish_filters,
            filter_indexes,
            retained_publishes,
            read_state: Default::default(),
        })
    }

    pub fn meter(&self, filter: &str) -> Option<SubscriptionMeter> {
        self.native
            .get(*self.filter_indexes.get(filter)?)
            .map(|data| data.meter.clone())
    }

    pub fn waiters(&self, filter: &Filter) -> Option<&Waiters<DataRequest>> {
        self.native
            .get(*self.filter_indexes.get(filter)?)
            .map(|data| &data.waiters)
    }

    pub fn remove_waiters_for_id(
        &mut self,
        id: ConnectionId,
        filter: &Filter,
    ) -> Option<DataRequest> {
        let data = self
            .native
            .get_mut(*self.filter_indexes.get(filter)?)
            .unwrap();
        let waiters = data.waiters.get_mut();

        waiters
            .iter()
            .position(|&(conn_id, _)| conn_id == id)
            .and_then(|index| {
                waiters
                    .swap_remove_back(index)
                    .map(|(_, data_req)| data_req)
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
                let data = Data::new(
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
        &self,
        filter_idx: FilterIdx,
        offset: Offset,
        len: u64,
    ) -> io::Result<(Position, Vec<Publish>)> {
        // unwrap to get index of `self.native` is fine here, because when a new subscribe packet
        // arrives in `Router::handle_device_payload`, it first calls the function
        // `next_native_offset` which creates a new commitlog if one doesn't exist. So any new
        // reads will definitely happen on a valid filter.
        let data = self.native.get(filter_idx).unwrap();
        let mut o = Vec::new();
        // TODO: `readv` is infallible but its current return type does not
        // reflect that. Consequently, this method is also infallible.
        // Encoding this information is important so that calling function
        // has more information on how this method behaves.
        let next = data.log.readv(offset, len, &mut o)?;
        Ok((next, o))
    }

    pub fn shadow(&mut self, filter: &str) -> Option<Publish> {
        let data = self.native.get_mut(*self.filter_indexes.get(filter)?)?;
        data.log.last()
    }

    /// This method is called when the subscriber has caught up with the commit log. In which case,
    /// instead of actively checking for commits in each `Router::run_inner` iteration, we instead
    /// wait and only try reading again when new messages have been added to the commit log. This
    /// methods converts a `DataRequest` (which actively reads the commit log in `Router::consume`)
    /// to a `Waiter` (which only reads when notified).
    pub fn park(&mut self, id: ConnectionId, request: DataRequest) {
        // calling unwrap on index here is fine, because only place this function is called is in
        // `Router::consume` method, when the status after reading from commit log of the same
        // filter as `request` is "done", that is, the subscriber has caught up. In other words,
        // there has been atleast 1 call to `native_readv` for the same filter, which means if
        // `native_readv` hasn't paniced, so this won't panic either.
        let data = self.native.get_mut(request.filter_idx).unwrap();
        data.waiters.register(id, request);
    }

    /// Cleanup a connection from all the waiters
    pub fn clean(&mut self, id: ConnectionId) -> Vec<DataRequest> {
        let mut inflight = Vec::new();
        for (_, data) in self.native.iter_mut() {
            inflight.append(&mut data.waiters.remove(id));
        }

        inflight
    }

    pub fn insert_to_retained_publishes(&mut self, publish: Publish, topic: Topic) {
        self.retained_publishes.insert(topic, publish);
    }

    pub fn remove_from_retained_publishes(&mut self, topic: Topic) {
        self.retained_publishes.remove(&topic);
    }

    pub fn handle_retained_messages(
        &mut self,
        filter: &str,
        notifications: &mut VecDeque<(ConnectionId, DataRequest)>,
    ) {
        trace!(info = "retain-msg", filter = &filter);

        let idx = self.filter_indexes.get(filter).unwrap();

        let datalog = self.native.get_mut(*idx).unwrap();

        for (topic, publish) in self.retained_publishes.iter_mut() {
            if matches(topic, filter) {
                datalog.append(publish.clone(), notifications);
            }
        }
    }
}

pub struct Data<T> {
    filter: Filter,
    log: CommitLog<T>,
    waiters: Waiters<DataRequest>,
    meter: SubscriptionMeter,
}

impl<T> Data<T>
where
    T: Storage + Clone,
{
    fn new(filter: &str, max_segment_size: usize, max_mem_segments: usize) -> Data<T> {
        let log = CommitLog::new(max_segment_size, max_mem_segments).unwrap();

        let waiters = Waiters::with_capacity(10);
        let metrics = SubscriptionMeter::default();
        Data {
            filter: filter.to_owned(),
            log,
            waiters,
            meter: metrics,
        }
    }

    /// Writes to all the filters that are mapped to this publish topic
    /// and wakes up consumers that are matching this topic (if they exist)
    pub fn append(
        &mut self,
        item: T,
        notifications: &mut VecDeque<(ConnectionId, DataRequest)>,
    ) -> (Offset, &Filter) {
        let size = item.size();
        let offset = self.log.append(item);
        if let Some(mut parked) = self.waiters.take() {
            notifications.append(&mut parked);
        }

        self.meter.count += 1;
        self.meter.append_offset = offset;
        self.meter.total_size += size;
        self.meter.head_and_tail_id = self.log.head_and_tail();

        (offset, &self.filter)
    }
}

/// Acks log for a subscription
#[derive(Debug)]
pub struct AckLog {
    // Committed acks per connection. First pkid, last pkid, data
    committed: VecDeque<Ack>,
    // Recorded qos 2 publishes
    recorded: VecDeque<Publish>,
    acker: Acker,
}

/// Offset map, for topic T:
///
/// - For N publishes on T with M matching filters, we store the offsets
/// in a map. The i, j value of map gives the offset of i-th publish on
/// j-th filter's append log.
/// - We also store markers per filter. For j-th filter, the marker tells
/// what was the last persisted offset.
/// - Based on the markers of all filters in an offset map we calculate a
/// threshold. Threshold points to the latest publish packet that has been
/// persisted by all the subscribers.
///
/// From the datalog we receive information that F_i has been updated.
/// On update of marker of F_i, we recompute threshold for all offset
/// maps that have F_i in it and release the pubacks based new threshold.  
///
/// Example:
/// For publish on topic a/b/c we have have the following offset map.
/// Topic a/b/c maps to the filters a/b/c, a/+/c, and a/#. We have 4
/// publishes on 3 filters and marker (denoted by ←) corresponding to
/// every filter. Puback for P_0 is already released.
///
/// At t = 0,
///
///                 filters ➡️
/// time, publishes
///  ↓        ↓  
///               |  F_0     F_1     F_2
///               |  a/b/c   a/+/c   a/#
/// --------------------------------------             
///  t_0     P_0  |  0 ←                   [x]  ⬅️ threshold
///
///
/// At t = 4,
///
///                 filters ➡️
/// time, publishes
///  ↓        ↓  
///               |  F_0     F_1     F_2
///               |  a/b/c   a/+/c   a/#
/// --------------------------------------             
///  t_0     P_0  |  0 ←                   [x]  ⬅️ threshold
///  t_1     P_1  |  1       2               
///  t_2     P_2  |  2       7       
///  t_3     P_3  |  3       10      22
///
/// Lets say markers are updated at t = 4 as:
///
///           | old     new
///     --------------------
///      F_0  | 0       3
///      F_1  | N/A     8
///      F_2  | N/A     24
///
/// We recompute the threshold and release pubacks for P_1 and P_2:
///
///                 filters ➡️
/// time, publishes
///  ↓        ↓  
///               |  F_0     F_1     F_2
///               |  a/b/c   a/+/c   a/#
/// --------------------------------------             
///  t_0     P_0  |  0                          ⬅️ old threshold
///  t_1     P_1  |  1       2         
///  t_2     P_2  |  2       7 ←                ⬅️ new threshold
///  t_3     P_3  |  3 ←     10      22 ←
///   
// #[derive(Debug)]
// struct OffsetMap {
//     // subscribers end
//     // where for each filter's all subscribers have read till
//     filter_markers: HashMap<FilterIdx, Offset>,
//     //
//     filter_to_topics: HashMap<FilterIdx, Vec<Topic>>,
//     topic_to_filters: HashMap<Topic, Vec<FilterIdx>>,
//     filter_publish_offsets: HashMap<FilterIdx, VecDeque<Offset>>,

//     publishers_markers: HashMap<Topic, Offset>,
//     pending_pubacks: HashMap<Topic, VecDeque<(usize, PubAck)>>,

//     topic_read_map: HashMap<Topic, PublishState>,
// }

impl PublishState {
    fn update(&mut self, puback: PubAck, updates: HashMap<FilterIdx, Offset>) {
        self.pubacks.push(puback);

        let map_width = match self.publish_offsets.first() {
            Some(first) => first.len(),
            None => 0,
        };

        for (filter_id, updated_offset) in updates {
            // check if filter id is in the mapping
            // if not present then that means the update is new
            // this means we need to add a new row to the map
            // and set the mapping for the filter_id

            let maybe_id = self.filter_to_row_id.get(&filter_id);
            if let Some(&id) = maybe_id {
                // already the filter is present in map
                let t = self.publish_offsets.get_mut(id).unwrap();
                t.push(Some(updated_offset));
            } else {
                // new filter!
                let mut new_row = vec![None; map_width];
                new_row.push(Some(updated_offset));
                self.publish_offsets.push(new_row);
                self.filter_to_row_id
                    .insert(filter_id, self.publish_offsets.len() - 1);
            }
        }

        for row in self.publish_offsets.iter_mut() {
            row.resize(map_width + 1, None);
            println!("{row:?}");
        }
    }

    fn recompute_read_marker(&mut self, filter_thresholds: &HashMap<FilterIdx, Offset>) {
        let mut markers = Vec::new();

        for (filter_idx, &filter_threshold) in filter_thresholds {
            let filter_row = self
                .filter_to_row_id
                .get(filter_idx)
                .expect("unexpected: filter id should be present in map");
            let row = self.publish_offsets.get(*filter_row).unwrap();

            let mut threshold = None;
            for (col, publish_offset) in row.iter().enumerate() {
                if publish_offset.is_some() && publish_offset > &Some(filter_threshold) {
                    break;
                };
                threshold = Some(col)
            }

            markers.push(threshold);
        }

        self.read_marker_column = markers.into_iter().min().unwrap();
    }
}

// impl OffsetMap {
//     pub fn update(&mut self, filter_id: FilterIdx, marker: Offset) -> Result<(), String> {
//         // update that filter's subscriber marker
//         self.filter_markers.entry(filter_id).or_insert(marker);

//         // get topics of updated filter
//         let topics = match self.filter_to_topics.get(&filter_id) {
//             Some(topics) => topics,
//             None => return Err("filter does not map to any topic".into()),
//         };

//         let mut freed_pubacks = Vec::<PubAck>::new();
//         for topic in topics {
//             let read_map = match self.topic_read_map.get_mut(topic) {
//                 Some(read_map) => read_map,
//                 None => return Err("topic does not map to any filter".into()),
//             };

//             let prev_read_marker = read_map.read_marker_column;
//             read_map.recompute_read_marker(&self.filter_markers);
//             let new_read_marker = read_map.read_marker_column;

//             // release the pubacks
//             let mut eligible_pubacks =
//                 read_map.pubacks[prev_read_marker - 1..=new_read_marker].to_vec();
//             freed_pubacks.append(&mut eligible_pubacks);
//         }
//         Ok(())
//     }
// }

impl AckLog {
    /// New log
    pub fn new(read_state: Arc<Mutex<ReadState>>) -> AckLog {
        AckLog {
            committed: VecDeque::with_capacity(100),
            recorded: VecDeque::with_capacity(100),
            acker: Acker::new(read_state),
        }
    }

    pub fn connack(&mut self, id: ConnectionId, ack: ConnAck) {
        let ack = Ack::ConnAck(id, ack);
        self.committed.push_back(ack);
    }

    pub fn suback(&mut self, ack: SubAck) {
        let ack = Ack::SubAck(ack);
        self.committed.push_back(ack);
    }

    fn puback(&mut self, ack: PubAck) {
        let ack = Ack::PubAck(ack);
        self.committed.push_back(ack);
    }

    pub fn pubrec(&mut self, publish: Publish, ack: PubRec) {
        let ack = Ack::PubRec(ack);
        self.recorded.push_back(publish);
        self.committed.push_back(ack);
    }

    pub fn pubrel(&mut self, ack: PubRel) {
        let ack = Ack::PubRel(ack);
        self.committed.push_back(ack);
    }

    pub fn pubcomp(&mut self, ack: PubComp) -> Option<Publish> {
        let ack = Ack::PubComp(ack);
        self.committed.push_back(ack);
        self.recorded.pop_front()
    }

    pub fn pingresp(&mut self, ack: PingResp) {
        let ack = Ack::PingResp(ack);
        self.committed.push_back(ack);
    }

    pub fn unsuback(&mut self, ack: UnsubAck) {
        let ack = Ack::UnsubAck(ack);
        self.committed.push_back(ack);
    }

    pub fn readv(&mut self) -> &mut VecDeque<Ack> {
        &mut self.committed
    }

    pub fn insert_pending_acks(
        &mut self,
        publish: Publish,
        publish_offsets: HashMap<usize, Offset>,
    ) {
        self.acker.register_write(
            String::from_utf8(publish.topic.to_vec()).unwrap(),
            PubAck {
                pkid: publish.pkid,
                reason: crate::protocol::PubAckReason::Success,
            },
            publish_offsets,
        )
    }

    pub fn release_pending_acks(&mut self) {
        let mut pubacks = self
            .acker
            .release_acks()
            .into_iter()
            .map(Ack::PubAck)
            .collect();

        self.committed.append(&mut pubacks);
    }
}

#[cfg(test)]
mod test {
    use parking_lot::Mutex;

    use super::{DataLog, PublishState};
    use crate::{
        protocol::{PubAck, Publish},
        router::{
            logs::{Acker, ReadState},
            FilterIdx,
        },
        Offset, RouterConfig,
    };
    use std::{collections::HashMap, sync::Arc, vec};

    #[test]
    fn publish_filters_updating_correctly_on_new_topic_subscription() {
        let config = RouterConfig {
            instant_ack: true,
            max_segment_size: 1024,
            max_connections: 10,
            max_segment_count: 10,
            max_read_len: 1024,
            initialized_filters: None,
        };
        let mut data = DataLog::new(config).unwrap();
        data.next_native_offset("topic/a");
        data.matches("topic/a");

        data.next_native_offset("topic/+");

        assert_eq!(data.publish_filters.get("topic/a").unwrap().len(), 2);
    }

    #[test]
    fn publish_filters_updating_correctly_on_new_publish() {
        let config = RouterConfig {
            instant_ack: true,
            max_segment_size: 1024,
            max_connections: 10,
            max_segment_count: 10,
            max_read_len: 1024,
            initialized_filters: None,
        };
        let mut data = DataLog::new(config).unwrap();
        data.next_native_offset("+/+");

        data.matches("topic/a");

        assert_eq!(data.publish_filters.get("topic/a").unwrap().len(), 1);
    }

    #[test]
    fn test_offset_map_updates() {
        // Expected state of map with every update
        //
        // t_0:
        //
        //    f_0 [(0, 0)]
        //
        // t_1:
        //
        //    f_0 [(0,0),  (1,5) ]
        //    f_2 [ x,     (0,10)]
        //
        // t_2:
        //
        //    f_0 [(0,0),  (1,5),   (1,8) ]
        //    f_2 [ x,     (0,10),  (1,12)]
        //    f_5 [ x,      x,      (1,3) ]
        //
        // t_3:
        //    f_0 [(0,0),  (1,5),   (1,8),    x,   ]
        //    f_2 [ x,     (0,10),  (1,12),  (1,15)]
        //    f_5 [ x,      x,      (1,3),    x,   ]
        //
        // t_4:
        //
        //    f_0 [(0,0),  (1,5),   (1,8),    x,      (2,11)]
        //    f_2 [ x,     (0,10),  (1,12),  (1,15),   x    ]
        //    f_5 [ x,      x,      (1,3),    x,      (1,7) ]
        //    f_6 [ x,      x,       x,       x,      (0,4) ]

        let filter_to_index_mapping = HashMap::new();
        let map = Vec::<Vec<Option<Offset>>>::new();

        let mut publish_state = PublishState {
            publish_offsets: map,
            filter_to_row_id: filter_to_index_mapping,
            pubacks: Vec::new(),
            read_marker_column: None,
        };

        let updates = vec![
            HashMap::from([(0, (0, 0))]),
            HashMap::from([(0, (1, 5)), (2, (0, 10))]),
            HashMap::from([(0, (1, 8)), (2, (1, 12)), (5, (1, 3))]),
            HashMap::from([(2, (1, 15))]),
            HashMap::from([(0, (2, 11)), (5, (1, 7)), (6, (0, 4))]),
        ];

        let publishes = (0..updates.len())
            .map(|id| {
                (
                    PubAck {
                        pkid: id as u16,
                        reason: crate::protocol::PubAckReason::Success,
                    },
                    updates[id].clone(),
                )
            })
            .collect::<Vec<(PubAck, HashMap<FilterIdx, Offset>)>>();

        for publish in publishes {
            publish_state.update(publish.0, publish.1);
        }

        assert_eq!(
            publish_state.publish_offsets,
            vec![
                vec![
                    Some((0, 0)),
                    Some((1, 5)),
                    Some((1, 8)),
                    None,
                    Some((2, 11))
                ],
                vec![None, Some((0, 10)), Some((1, 12)), Some((1, 15)), None],
                vec![None, None, Some((1, 3)), None, Some((1, 7))],
                vec![None, None, None, None, Some((0, 4))],
            ]
        );
    }

    #[test]
    fn test_topic_marker_calculation() {
        let read_state = Arc::new(Mutex::from(ReadState::default()));

        // subscriber - 0
        // reading on filter - 0
        // read offset - 0, 0
        read_state.lock().register_read(0, 0, (0, 0));
        // subscriber - 1
        // reading on filter - 1
        // read offset - 0, 0
        read_state.lock().register_read(1, 1, (0, 0));

        let mut acker = Acker::new(read_state.clone());

        // publisher - 1
        // publishing on topic "A" that matches filter - 0 and 1
        // on filter 0 offset is (0, 5)
        // on filter 1 offset is (1, 10)
        acker.register_write(
            "A".into(),
            PubAck {
                pkid: 0,
                reason: crate::protocol::PubAckReason::Success,
            },
            HashMap::from([(0, (0, 5)), (1, (1, 10))]),
        );

        acker.register_write(
            "A".into(),
            PubAck {
                pkid: 1,
                reason: crate::protocol::PubAckReason::Success,
            },
            HashMap::from([(0, (0, 8)), (1, (1, 15))]),
        );

        let acks = acker.release_acks();
        println!("{acks:?}");

        // subscriber - 0
        // reading on filter - 0
        // read offset - 0, 0
        read_state.lock().register_read(0, 0, (0, 7));
        // subscriber - 1
        // reading on filter - 1
        // read offset - 0, 0
        read_state.lock().register_read(1, 1, (1, 11));

        let acks = acker.release_acks();
        // println!("{read_state:?}");
        println!("{acks:?}");

        // let publish_offsets = vec![vec![Some((0, 0))]];
        // let filter_to_row_id = HashMap::from([(0, 0)]);
        // let pubacks = vec![PubAck {
        //     pkid: 0,
        //     reason: crate::protocol::PubAckReason::Success,
        // }];
        // let read_marker_column = 0;

        // let mut topic_state_read_map = PublishState {
        //     publish_offsets,
        //     filter_to_row_id,
        //     pubacks,
        //     read_marker_column,
        // };

        // let puback_updates = vec![
        //     (
        //         PubAck {
        //             pkid: 1,
        //             reason: crate::protocol::PubAckReason::Success,
        //         },
        //         HashMap::from([(0, (0, 1)), (1, (0, 2))]),
        //     ),
        //     (
        //         PubAck {
        //             pkid: 2,
        //             reason: crate::protocol::PubAckReason::Success,
        //         },
        //         HashMap::from([(0, (0, 2)), (1, (0, 7))]),
        //     ),
        //     (
        //         PubAck {
        //             pkid: 3,
        //             reason: crate::protocol::PubAckReason::Success,
        //         },
        //         HashMap::from([(0, (0, 3)), (1, (0, 10)), (2, (0, 22))]),
        //     ),
        // ];

        // let prev = topic_state_read_map.read_marker_column;
        // for (puback, update) in puback_updates {
        //     topic_state_read_map.update(puback, update)
        // }

        // for row in &topic_state_read_map.publish_offsets {
        //     println!("{:?}", row);
        // }

        // let updated_thresholds = HashMap::from([(0, (0, 3)), (1, (0, 11)), (2, (0, 24))]);
        // topic_state_read_map.recompute_read_marker(&updated_thresholds);

        // let curr = topic_state_read_map.read_marker_column;

        // assert_eq!(prev, 0);
        // assert_eq!(curr, 3);
    }

    //     #[test]
    //     fn appends_are_written_to_correct_commitlog() {
    //         pretty_env_logger::init();
    //         let config = RouterConfig {
    //             instant_ack: true,
    //             max_segment_size: 1024,
    //             max_connections: 10,
    //             max_mem_segments: 10,
    //             max_disk_segments: 0,
    //             max_read_len: 1024,
    //             log_dir: None,
    //             dynamic_log: true,
    //         };

    //         let mut data = DataLog::new(config).unwrap();
    //         data.next_native_offset("/devices/2321/actions");
    //         for i in 0..2 {
    //             let publish = Publish::new("/devices/2321/events/imu/jsonarray", QoS::AtLeastOnce, vec![1, 2, 3]);
    //             let v = data.native_append(publish);
    //             dbg!(v);
    //         }

    //         for i in 0..2 {
    //             let publish = Publish::new("/devices/2321/actions", QoS::AtLeastOnce, vec![1, 2, 3]);
    //             let v = data.native_append(publish);
    //             dbg!(v);
    //         }
    //     }
}
