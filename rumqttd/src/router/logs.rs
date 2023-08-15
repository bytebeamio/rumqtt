use super::Ack;
use slab::Slab;
use tracing::{info, trace};

use crate::protocol::{
    matches, ConnAck, ConnAckProperties, PingResp, PubAck, PubComp, PubRec, PubRel, Publish,
    PublishProperties, SubAck, UnsubAck,
};
use crate::router::{DataRequest, FilterIdx, SubscriptionMeter, Waiters};
use crate::{ConnectionId, Filter, Offset, RouterConfig, Topic};

use crate::segments::{CommitLog, Position};
use crate::Storage;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::time::Instant;

type PubWithProp = (Publish, Option<PublishProperties>);

#[derive(Clone)]
pub struct PublishData {
    pub publish: Publish,
    pub properties: Option<PublishProperties>,
    pub timestamp: Instant,
}

impl From<PubWithProp> for PublishData {
    fn from((publish, properties): PubWithProp) -> Self {
        PublishData {
            publish,
            properties,
            timestamp: Instant::now(),
        }
    }
}

// TODO: remove this from here
impl Storage for PublishData {
    // TODO: calculate size of publish properties as well!
    fn size(&self) -> usize {
        let publish = &self.publish;
        4 + publish.topic.len() + publish.payload.len()
    }
}

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
    pub native: Slab<Data<PublishData>>,
    /// Map of subscription filter name to filter index
    filter_indexes: HashMap<Filter, FilterIdx>,
    retained_publishes: HashMap<Topic, PublishData>,
    /// List of filters associated with a topic
    publish_filters: HashMap<Topic, Vec<FilterIdx>>,
}

impl DataLog {
    pub fn new(config: RouterConfig) -> io::Result<DataLog> {
        let mut native = Slab::new();
        let mut filter_indexes = HashMap::new();
        let retained_publishes = HashMap::new();
        let publish_filters = HashMap::new();

        if let Some(warmup_filters) = config.initialized_filters.clone() {
            for filter in warmup_filters {
                let data = Data::new(&filter, &config);

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
        })
    }

    pub fn meter(&mut self, filter: &str) -> Option<&mut SubscriptionMeter> {
        let data = self.native.get_mut(*self.filter_indexes.get(filter)?)?;
        Some(&mut data.meter)
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
                let data = Data::new(filter, &self.config);

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
    ) -> io::Result<(Position, Vec<(PubWithProp, Offset)>)> {
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

        let now = Instant::now();
        o.retain_mut(|(pubdata, _)| {
            // Keep data if no properties exists, which implies no message expiry!
            let Some(properties) = pubdata.properties.as_mut() else {
                return true
            };

            // Keep data if there is no message_expiry_interval
            let Some(message_expiry_interval) = properties.message_expiry_interval.as_mut() else {
                return true
            };

            let time_spent = (now - pubdata.timestamp).as_secs() as u32;

            let is_valid = time_spent < *message_expiry_interval;

            // ignore expired messages
            if is_valid {
                // set message_expiry_interval to (original value - time spent waiting in server)
                // ref: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901112
                *message_expiry_interval -= time_spent;
            }

            is_valid
        });

        // no need to include timestamp when returning
        let o = o
            .into_iter()
            .map(|(pubdata, offset)| ((pubdata.publish, pubdata.properties), offset))
            .collect();

        Ok((next, o))
    }

    pub fn shadow(&mut self, filter: &str) -> Option<PubWithProp> {
        let data = self.native.get_mut(*self.filter_indexes.get(filter)?)?;
        data.log.last().map(|p| (p.publish, p.properties))
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

    pub fn insert_to_retained_publishes(
        &mut self,
        publish: Publish,
        publish_properties: Option<PublishProperties>,
        topic: Topic,
    ) {
        let pub_with_props = (publish, publish_properties);
        self.retained_publishes.insert(topic, pub_with_props.into());
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
    pub log: CommitLog<T>,
    pub waiters: Waiters<DataRequest>,
    meter: SubscriptionMeter,
}

impl<T> Data<T>
where
    T: Storage + Clone,
{
    pub fn new(filter: &str, router_config: &RouterConfig) -> Data<T> {
        let mut max_segment_size = router_config.max_segment_size;
        let mut max_mem_segments = router_config.max_segment_count;

        // Override segment config for selected filter
        if let Some(config) = &router_config.custom_segment {
            for (f, segment_config) in config {
                if matches(filter, f) {
                    info!("Overriding segment config for filter: {}", filter);
                    max_segment_size = segment_config.max_segment_size;
                    max_mem_segments = segment_config.max_segment_count;
                }
            }
        }

        // max_segment_size: usize, max_mem_segments: usize
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
        self.meter.total_size += size;

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
}

impl AckLog {
    /// New log
    pub fn new() -> AckLog {
        AckLog {
            committed: VecDeque::with_capacity(100),
            recorded: VecDeque::with_capacity(100),
        }
    }

    pub fn connack(&mut self, id: ConnectionId, ack: ConnAck, props: Option<ConnAckProperties>) {
        let ack = Ack::ConnAck(id, ack, props);
        self.committed.push_back(ack);
    }

    pub fn suback(&mut self, ack: SubAck) {
        let ack = Ack::SubAck(ack);
        self.committed.push_back(ack);
    }

    pub fn puback(&mut self, ack: PubAck) {
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
}

#[cfg(test)]
mod test {
    use super::DataLog;
    use crate::router::shared_subs::Strategy;
    use crate::RouterConfig;

    #[test]
    fn publish_filters_updating_correctly_on_new_topic_subscription() {
        let config = RouterConfig {
            max_segment_size: 1024,
            max_connections: 10,
            max_segment_count: 10,
            max_outgoing_packet_count: 1024,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Strategy::RoundRobin,
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
            max_segment_size: 1024,
            max_connections: 10,
            max_segment_count: 10,
            max_outgoing_packet_count: 1024,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Strategy::RoundRobin,
        };
        let mut data = DataLog::new(config).unwrap();
        data.next_native_offset("+/+");

        data.matches("topic/a");

        assert_eq!(data.publish_filters.get("topic/a").unwrap().len(), 1);
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
