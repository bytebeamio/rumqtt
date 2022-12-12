use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use tracing::debug;

use crate::{protocol::PubAck, ConnectionId, Offset, Topic};

use super::FilterIdx;

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

/// TODO: Redo this
///
/// Publish offset map, for topic T:
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
#[derive(Default, Debug)]
pub struct PublishState {
    publish_offsets: Vec<Vec<Option<Offset>>>,
    filter_to_row_id: HashMap<FilterIdx, usize>,
    pubacks: Vec<PubAck>,
    read_marker_column: Option<usize>,
}

impl PublishState {
    fn update(&mut self, puback: PubAck, updates: HashMap<FilterIdx, Offset>) {
        self.pubacks.push(puback);

        println!("{:?}", self.publish_offsets);

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
            let filter_row = match self.filter_to_row_id.get(filter_idx) {
                Some(filter_row) => filter_row,
                None => {
                    debug!("This filter was not being published.");
                    continue;
                }
            };

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

#[cfg(test)]
mod test {
    use super::*;

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

    #[test]
    fn puback_release_test() {
        // S1 subscribes on filter F1 with read marker: (0, 0), and on filter F2 with read marker: (0, 0)
        let read_state = Arc::new(Mutex::from(ReadState::default()));
        read_state.lock().register_read(0, 0, (0, 0));
        read_state.lock().register_read(1, 0, (0, 0));

        // P1 publishes 5 messages on topic T. T maps to F1 and F2. Publish offset on F1: (0, 5), and on F2: (0, 5)
        let mut acker = Acker::new(read_state.clone());
        for i in 1..=5 {
            acker.register_write(
                "hello/world".into(),
                PubAck {
                    pkid: i,
                    reason: crate::protocol::PubAckReason::Success,
                },
                HashMap::from([(0, (0_u64, i.into())), (1, (0_u64, i.into()))]),
            );
        }

        // S2 subscribes on F2 with read marker: (0, 5)
        read_state.lock().register_read(1, 1, (0, 5));

        // ASSERT -> P1 gets ack for 0 publishes ()
        let acks = acker.release_acks();
        println!("{acks:?}");
        assert_eq!(acks.len(), 0);

        // S1 reads 4 messages from F1 with updated read marker: (0, 4), and 3 messages from F2 with updated read marker: (0, 3)
        read_state.lock().register_read(0, 0, (0, 4));
        read_state.lock().register_read(1, 0, (0, 3));

        // ASSERT -> P1 gets ack for 3 publishes (A1, A2, A3)
        let acks = acker.release_acks();
        println!("{acks:?}");
        assert_eq!(acks.len(), 3);

        // P1 publishes 5 messages on topic T. T maps to F1 and F2. Publish offset on F1: (0, 5), and on F2: (0, 5)
        for i in 6..=10 {
            acker.register_write(
                "hello/world".into(),
                PubAck {
                    pkid: i,
                    reason: crate::protocol::PubAckReason::Success,
                },
                HashMap::from([(0, (0_u64, i.into())), (1, (0_u64, i.into()))]),
            );
        }

        // S1 reads 3 messages from F1 with updated read marker: (0, 7), and 5 messages from F2 with updated read marker: (0, 8)
        read_state.lock().register_read(0, 0, (0, 7));
        read_state.lock().register_read(1, 0, (0, 8));

        // ASSERT -> P1 gets ack for 2 publishes (A4, A5)
        let acks = acker.release_acks();
        println!("{acks:?}");
        assert_eq!(acks.len(), 2);

        // S2 reads 2 messages from F2 with updated read marker: (0, 7)
        read_state.lock().register_read(1, 1, (0, 7));

        // ASSERT -> P1 gets ack for 2 publishes (A6, A7)
        let acks = acker.release_acks();
        println!("{acks:?}");
        assert_eq!(acks.len(), 2);

        // S1 reads 3 messages from F1 with updated read marker: (0, 10), and 2 messages from F2 with updated read marker: (0, 10)
        read_state.lock().register_read(0, 0, (0, 10));
        read_state.lock().register_read(1, 0, (0, 10));

        // ASSERT -> P1 gets ack for 0 publishes ()
        let acks = acker.release_acks();
        println!("{acks:?}");
        assert_eq!(acks.len(), 0);

        // S2 reads 3 messages from F2 with updated read marker: (0, 10)
        read_state.lock().register_read(1, 1, (0, 10));

        // ASSERT -> P1 gets ack for 3 publishes (A8, A9, A10)
        let acks = acker.release_acks();
        println!("{acks:?}");
        assert_eq!(acks.len(), 3);
    }
}
