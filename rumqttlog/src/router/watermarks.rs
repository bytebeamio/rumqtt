use std::collections::VecDeque;
use std::mem;

type Pkid = u16;
type Offset = u64;

/// Watermarks for a given topic
pub struct Watermarks {
    /// Topic this watermarks is tracking. This is only for debug prints
    topic: String,
    /// Replication count for this topic
    replication: usize,
    /// A map of packet ids and commitlog offsets for a given connection (identified by index)
    pub(crate) pkid_offset_map: Vec<Option<(VecDeque<Pkid>, VecDeque<Offset>)>>,
    /// Offset till which replication has happened (per mesh node)
    cluster_offsets: Vec<Offset>
}

impl Watermarks {
    pub fn new(topic: &str, replication: usize, max_connections: usize) -> Watermarks {
        Watermarks {
            topic: topic.to_owned(),
            replication,
            pkid_offset_map: vec![None; max_connections],
            cluster_offsets: vec![0, 0, 0]
        }
    }

    pub fn update_cluster_offsets(&mut self, id: usize, offset: u64) {
        if let Some(position) = self.cluster_offsets.get_mut(id) {
            *position = offset
        } else {
            panic!("We only support a maximum of 3 nodes at the moment. Received id = {}", id);
        }

        debug!("Updating cluster offsets. Topic = {}, Offsets: {:?}", self.topic, self.cluster_offsets);
    }

    pub fn acks(&mut self, id: usize) -> VecDeque<Pkid> {
        let is_replica = id < 10;
        // ack immediately if replication factor is 0 or if acks are being asked by a
        // replicator connection
        if self.replication == 0 || is_replica {
            if let Some(connection) = self.pkid_offset_map.get_mut(id).unwrap() {
                let new_ids = (VecDeque::new(), VecDeque::new());
                let (pkids, _offsets) = mem::replace(connection, new_ids);
                return pkids
            }
        } else {
            if let Some(connection) = self.pkid_offset_map.get_mut(id).unwrap() {
                let highest_replicated_offset = *self.cluster_offsets.iter().max().unwrap();

                // cut offsets which are less than highest replicated offset
                // e.g. For connection = 30, router id = 0, pkid_offset_map and replica offsets looks like this
                // pkid offset map = [5, 4, 3, 2, 1] : [15, 14, 10, 9, 8]
                // replica offsets = [0, 12, 8] implies replica 1 has replicated till 12 and replica 2 till 8
                // the above example should return pkids [5, 4]

                // get index of offset less than replicated offset and split there
                if let Some(index) = connection.1.iter().position(|x| *x <= highest_replicated_offset) {
                    connection.1.truncate(index);
                    let pkids = connection.0.split_off(index);
                    return pkids
                }
            }
        }

        return VecDeque::new()
    }

    pub fn update_pkid_offset_map(&mut self, id: usize, pkid: u16, offset: u64) {
        // connection ids which are greater than supported count should be rejected during
        // connection itself. Crashing here is a bug
        let connection = self.pkid_offset_map.get_mut(id).unwrap();
        let map = connection.get_or_insert((VecDeque::new(), VecDeque::new()));
        map.0.push_front(pkid);
        // save offsets only if replication > 0
        if self.replication > 0 {
            map.1.push_front(offset);
        }
    }
}

