mod connections;
mod data;
mod topics;

use crate::{Config, Data, DataRequest};
use bytes::Bytes;
use std::sync::Arc;

pub use connections::ConnectionsLog;
pub use topics::TopicsLog;

type Id = usize;

pub struct DataLog {
    id: Id,
    /// Commitlog by topic per replica. Commit log stores all the of given topic.
    /// Details are very similar to what kafka does. Who knows, we might
    /// even make the broker kafka compatible and directly feed it to databases
    commitlog: [data::DataLog; 3],
}

impl DataLog {
    pub fn new(id: Id, config: Arc<Config>) -> DataLog {
        let commitlog = [
            data::DataLog::new(config.clone(), 0),
            data::DataLog::new(config.clone(), 1),
            data::DataLog::new(config.clone(), 2),
        ];

        DataLog { id, commitlog }
    }

    /// Update matched topic offsets to current offset of this topic's commitlog
    pub fn seek_offsets_to_end(&self, id: Id, topic: &mut (String, u8, [(u64, u64); 3])) {
        let commitlog = &self.commitlog[id];
        commitlog.seek_offsets_to_end(topic);
    }

    /// Connections pull logs from both replication and connections where as replicator
    /// only pull logs from connections.
    /// Data from replicator and data from connection are separated for this reason
    pub fn append(&mut self, id: Id, topic: &str, bytes: Bytes) -> Option<(bool, (u64, u64))> {
        // id 0-10 are reserved for replications which are linked to other routers in the mesh
        let replication_data = id < 10;
        let commitlog = if replication_data {
            self.commitlog.get_mut(id).unwrap()
        } else {
            self.commitlog.get_mut(self.id).unwrap()
        };

        match commitlog.append(&topic, bytes) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("Commitlog append failed. Error = {:?}", e);
                None
            }
        }
    }

    pub fn retain(&mut self, id: Id, topic: &str, bytes: Bytes) -> Option<bool> {
        // id 0-10 are reserved for replications which are linked to other routers in the mesh
        let replication_data = id < 10;
        let commitlog = if replication_data {
            self.commitlog.get_mut(id).unwrap()
        } else {
            self.commitlog.get_mut(self.id).unwrap()
        };

        match commitlog.retain(&topic, bytes) {
            Ok(v) => Some(v),
            Err(e) => {
                error!("Commitlog append failed. Error = {:?}", e);
                None
            }
        }
    }

    pub fn handle_data_request(&mut self, id: Id, request: &DataRequest) -> Option<Data> {
        // Replicator asking data implies that previous data has been replicated
        // We update replication watermarks at this point
        // Also, extract only connection data if this request is from a replicator
        if id < 10 {
            self.extract_connection_data(request)
        } else {
            self.extract_all_data(request)
        }
    }

    /// Extracts data from native log. Returns None in case the
    /// log is caught up or encountered an error while reading data
    pub(crate) fn extract_connection_data(&mut self, request: &DataRequest) -> Option<Data> {
        let native_id = self.id;
        let topic = &request.topic;
        let last_retain = request.last_retain;
        let cursors = request.cursors;

        let commitlog = &mut self.commitlog[native_id];

        let (segment, offset) = cursors[native_id];
        let mut reply = Data::new(
            request.topic.clone(),
            request.qos,
            cursors,
            last_retain,
            0,
            Vec::new(),
        );

        match commitlog.readv(topic, segment, offset, last_retain) {
            Ok(Some((jump, base_offset, record_offset, last_retain, payload))) => {
                match jump {
                    Some(next) => reply.cursors[native_id] = (next, next),
                    None => reply.cursors[native_id] = (base_offset, record_offset),
                }

                // Update retain id (incase readv has retained publish to consider)
                reply.last_retain = last_retain;

                // Update reply's cursors only when read has returned some data
                // Move the reply to next segment if we are done with the current one
                if payload.is_empty() {
                    return None;
                }
                reply.payload = payload;
                Some(reply)
            }
            Ok(None) => None,
            Err(e) => {
                error!("Failed to extract data from commitlog. Error = {:?}", e);
                None
            }
        }
    }

    /// Extracts data from native and replicated logs. Returns None in case the
    /// log is caught up or encountered an error while reading data
    pub(crate) fn extract_all_data(&mut self, request: &DataRequest) -> Option<Data> {
        let topic = &request.topic;
        let mut last_retain = request.last_retain;
        let mut cursors = [(0, 0); 3];
        let mut payload = Vec::new();

        // Iterate through native and replica commitlogs to collect data (of a topic)
        for (i, commitlog) in self.commitlog.iter_mut().enumerate() {
            let (segment, offset) = request.cursors[i];
            match commitlog.readv(topic, segment, offset, last_retain) {
                Ok(Some(v)) => {
                    let (jump, base_offset, record_offset, retain, mut data) = v;
                    match jump {
                        Some(next) => cursors[i] = (next, next),
                        None => cursors[i] = (base_offset, record_offset),
                    }

                    // Update retain id (incase readv has retained publish to consider)
                    last_retain = retain;

                    if data.is_empty() {
                        continue;
                    }
                    payload.append(&mut data);
                }
                Ok(None) => continue,
                Err(e) => {
                    error!("Failed to extract data from commitlog. Error = {:?}", e);
                }
            }
        }

        // When payload is empty due to read after current offset
        // because of uninitialized request, update request with
        // latest offsets and return None so that caller registers
        // the request with updated offsets
        match payload.is_empty() {
            true => None,
            false => Some(Data::new(
                request.topic.clone(),
                request.qos,
                cursors,
                last_retain,
                0,
                payload,
            )),
        }
    }
}
