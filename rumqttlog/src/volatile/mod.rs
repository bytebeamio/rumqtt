use fnv::FnvHasher;
use std::collections::HashMap;

mod index;
mod segment;

use bytes::Bytes;
use index::Index;
use segment::Segment;
use std::hash::BuildHasherDefault;
use std::io;

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

struct Chunk {
    index: Index,
    segment: Segment,
}

pub struct Log {
    /// ID of the last appended record
    last_record_id: u64,
    /// Maximum size of a segment
    max_segment_size: u64,
    /// Base offsets of all the segments
    base_offsets: Vec<u64>,
    /// Maximum number of segments
    max_segments: usize,
    /// Current active chunk to append
    active_chunk: u64,
    /// All the segments mapped with their base offsets
    /// TODO benchmark with id map
    chunks: HashMapFnv<u64, Chunk>,
}

impl Log {
    pub fn new(max_segment_size: u64, max_segments: usize) -> io::Result<Self> {
        if max_segment_size < 1024 {
            panic!("size should be at least 1KB")
        }

        let mut base_offsets = Vec::new();
        let mut chunks = HashMapFnv::default();

        let index = Index::new(0);
        let segment = Segment::new(0)?;
        let chunk = Chunk { index, segment };

        chunks.insert(0, chunk);
        base_offsets.push(0);

        let log = Log {
            last_record_id: 0,
            max_segment_size,
            max_segments,
            base_offsets,
            chunks,
            active_chunk: 0,
        };

        Ok(log)
    }

    pub fn active_chunk(&self) -> u64 {
        self.active_chunk
    }

    pub fn append(&mut self, record: Bytes) -> Result<u64, io::Error> {
        // TODO last_record_id and offset are same. Remove last_record_id
        let active_chunk = if let Some(v) = self.chunks.get_mut(&self.active_chunk) {
            v
        } else {
            return Err(io::Error::new(io::ErrorKind::Other, "No active segment"));
        };

        if active_chunk.segment.size() >= self.max_segment_size {
            // update active chunk
            let base_offset = active_chunk.index.base_offset() + active_chunk.index.count();
            let index = Index::new(base_offset);
            let segment = Segment::new(base_offset)?;
            let chunk = Chunk { index, segment };

            self.chunks.insert(base_offset, chunk);
            self.base_offsets.push(base_offset);
            self.active_chunk = base_offset;

            if self.base_offsets.len() > self.max_segments {
                let remove_offset = self.base_offsets.remove(0);
                self.chunks.remove(&remove_offset);
            }
        }

        // write record to segment and index
        let len = record.len() as u64;
        let active_chunk = self.chunks.get_mut(&self.active_chunk).unwrap();
        let (offset, _) = active_chunk.segment.append(record)?;
        self.last_record_id += 1;
        active_chunk.index.write(self.last_record_id, offset, len);
        Ok(self.last_record_id)
    }

    /// Read a record from correct segment
    /// Returns data, next base offset and relative offset
    pub fn read(&mut self, base_offset: u64, offset: u64) -> io::Result<(u64, Bytes)> {
        let chunk = match self.chunks.get_mut(&base_offset) {
            Some(segment) => segment,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid segment",
                ))
            }
        };

        let (offset, _len, id) = chunk.index.read(offset)?;
        let payload = chunk.segment.read(offset);
        Ok((id, payload))
    }

    /// Goes through index and returns chunks which tell how to sweep segments to collect
    /// necessary amount on data asked by the user
    /// Corner cases:
    /// When there is more data (in other segments) current eof should move to next segment
    /// Empty segments are possible after moving to next segment
    /// EOFs after some data is collected are not errors
    fn indexv(&self, segment: u64, offset: u64, size: u64) -> io::Result<(bool, Chunks)> {
        let mut done = false;
        let mut chunks = Chunks {
            segment,
            offset,
            count: 0,
            size: 0,
            ids: Vec::with_capacity(1000),
            chunks: Vec::new(),
        };

        loop {
            // Get the chunk with given base offset
            let chunk = match self.chunks.get(&chunks.segment) {
                Some(c) => c,
                None if chunks.count == 0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Invalid segment",
                    ))
                }
                None => {
                    done = true;
                    break;
                }
            };

            // If next relative offset is equal to index count => We've crossed the boundary
            if chunks.offset >= chunk.index.count() {
                // break if we are already at the tail segment
                if chunks.segment == *self.base_offsets.last().unwrap() {
                    chunks.offset -= 1;
                    done = true;
                    break;
                }

                // we use 'total offsets' to go next segment. this remains same during subsequent
                // tail reads if there are no appends. hence the above early return
                chunks.segment = chunk.index.base_offset() + chunk.index.count();
                chunks.offset = 0;
                continue;
            }

            // Get what to read from the segment and fill the buffer. Covers the case where the logic has just moved to next
            // segment and the segment is empty
            let read_size = size - chunks.size;
            let (position, payload_size, ids) = chunk.index.readv(chunks.offset, read_size)?;
            let count = ids.len() as u64;
            chunks.offset += count;
            chunks.count += count;
            chunks.size += payload_size;
            chunks.ids.extend(ids);
            chunks
                .chunks
                .push((chunks.segment, position, payload_size, count));
            if chunks.size >= size {
                chunks.offset -= 1;
                break;
            }
        }

        Ok((done, chunks))
    }

    /// Reads multiple packets from the storage and return base offset and relative offset of the
    /// Returns base offset, relative offset of the last record along with number of messages and count
    /// Goes to next segment when relative off set crosses boundary
    pub fn readv(
        &mut self,
        segment: u64,
        offset: u64,
        size: u64,
    ) -> io::Result<(bool, u64, u64, u64, Vec<u64>, Vec<Bytes>)> {
        // TODO We don't need Vec<u64>. Remove that from return
        let (done, chunks) = self.indexv(segment, offset, size)?;
        let mut out = Vec::new();

        for c in chunks.chunks {
            let chunk = match self.chunks.get_mut(&c.0) {
                Some(c) => c,
                None => break,
            };

            let o = chunk.segment.readv(c.1, c.3);
            out.extend(o);
        }

        Ok((
            done,
            chunks.segment,
            chunks.offset,
            chunks.size,
            chunks.ids,
            out,
        ))
    }
}

/// Captured state while sweeping indexes collect a bulk of records
/// from segment/segments
/// TODO: 'chunks' vector arguments aren't readable
#[derive(Debug)]
struct Chunks {
    segment: u64,
    offset: u64,
    count: u64,
    size: u64,
    /// All the identifiers of payloads to be collected from segments
    ids: Vec<u64>,
    /// Segment, offset, size, count of a chunk
    chunks: Vec<(u64, u64, u64, u64)>,
}

#[cfg(test)]
mod test {
    use super::Log;
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use std::io;

    #[test]
    fn append_creates_and_deletes_segments_correctly() {
        let mut log = Log::new(10 * 1024, 10).unwrap();
        let mut payload = vec![0u8; 1024];

        // 200 1K iterations. 20 files ignoring deletes. 0.segment, 10.segment .... 199.segment
        // considering deletes -> 110.segment .. 199.segment
        for i in 0..200 {
            payload[0] = i;
            let payload = Bytes::from(payload.clone());
            log.append(payload).unwrap();
        }

        // Semi fill 200.segment
        for i in 200..205 {
            payload[0] = i;
            let payload = Bytes::from(payload.clone());
            log.append(payload).unwrap();
        }

        let data = log.read(10, 0);
        match data {
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => (),
            _ => panic!("Expecting an invalid input error"),
        };

        // read segment with base offset 110
        let base_offset = 110;
        for i in 0..10 {
            let (id, data) = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
            assert_eq!(id, d as u64 + 1);
        }

        // read segment with base offset 190
        let base_offset = 110;
        for i in 0..10 {
            let (id, data) = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
            assert_eq!(id, d as u64 + 1);
        }

        // read 200.segment which is semi filled with 5 records
        let base_offset = 200;
        for i in 0..5 {
            let (id, data) = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
            assert_eq!(id, d as u64 + 1);
        }

        let data = log.read(base_offset, 5);
        match data {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => (),
            _ => panic!("Expecting end of file error"),
        };
    }

    #[test]
    fn multi_segment_reads_work_as_expected() {
        // 100K bytes
        let record_count = 100;
        let record_size = 1 * 1024;

        // 10 records per segment. 10 segments (0.segment - 90.segment)
        let max_segment_size = 10 * 1024;
        let mut log = Log::new(max_segment_size, 100).unwrap();

        // 100 1K iterations. 10 files ignoring deletes.
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 90.segment (90..99)
        // 10K per file
        let mut payload = vec![0u8; record_size];
        for i in 0..record_count {
            payload[0] = i as u8;
            let payload = Bytes::from(payload.clone());
            log.append(payload).unwrap();
        }

        // Read all the segments
        let base_offset = 0;
        for i in 0..10 {
            let (id, data) = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
            assert_eq!(id, d as u64 + 1);
        }
    }

    #[test]
    fn vectored_read_works_as_expected() {
        let mut log = Log::new(10 * 1024, 10).unwrap();

        // 90 1K iterations. 10 files ignoring deletes.
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 90.segment (0 size)
        // 10K per file
        let mut payload = vec![0u8; 1024];
        for i in 0..90 {
            payload[0] = i;
            let payload = Bytes::from(payload.clone());
            log.append(payload).unwrap();
        }

        // Read 50K. Reads 0.segment - 4.segment
        let (done, segment, offset, total_size, ids, _data) = log.readv(0, 0, 50 * 1024).unwrap();
        assert_eq!(segment, 40);
        assert_eq!(offset, 9);
        assert_eq!(ids.len(), 50);
        for i in 0..ids.len() {
            assert_eq!(i as u64 + 1, ids[i]);
        }
        assert_eq!(done, false);
        assert_eq!(total_size, 50 * 1024);

        // Read 50.segment offset 0
        let (id, data) = log.read(50, 0).unwrap();
        assert_eq!(data[0], 50);
        assert_eq!(id, 51);
    }

    #[test]
    fn vectored_reads_crosses_boundary_correctly() {
        let mut log = Log::new(10 * 1024, 10).unwrap();

        // 25 1K iterations. 3 segments
        // 0.segment (10K, data with 0 - 9), 10.segment (5K, data with 10 - 14)
        let mut payload = vec![0u8; 1024];
        for i in 0..25 {
            payload[0] = i;
            let payload = Bytes::from(payload.clone());
            log.append(payload).unwrap();
        }

        // Read 15K. Crosses boundaries of the segment and offset will be in the middle of 2nd segment
        let (done, segment, offset, total_size, ids, _data) = log.readv(0, 0, 15 * 1024).unwrap();
        assert_eq!(segment, 10);
        assert_eq!(offset, 4);
        assert_eq!(ids.len(), 15);
        for i in 0..ids.len() {
            assert_eq!(i as u64 + 1, ids[i])
        }
        assert_eq!(done, false);
        assert_eq!(total_size, 15 * 1024);

        // Read 15K. Crosses boundaries of the segment and offset will be at last record of 3rd segment
        let (done, segment, offset, total_size, ids, _data) =
            log.readv(segment, offset + 1, 15 * 1024).unwrap();
        assert_eq!(segment, 20);
        assert_eq!(offset, 4);
        assert_eq!(ids.len(), 10);
        for i in 0..ids.len() {
            let id = i + 15;
            assert_eq!(id as u64 + 1, ids[i])
        }
        assert_eq!(done, true);
        assert_eq!(total_size, 10 * 1024);
    }

    #[test]
    fn vectored_read_more_than_full_chomp_works_as_expected() {
        let mut log = Log::new(10 * 1024, 10).unwrap();

        // 90 1K iterations. 10 files
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 80.segment
        // 10K per file. 90K in total
        let mut payload = vec![0u8; 1024];
        for i in 0..90 {
            payload[0] = i;
            let payload = Bytes::from(payload.clone());
            log.append(payload).unwrap();
        }

        // Read 200K. Crosses boundaries of all the segments
        let (done, segment, offset, total_size, ids, _data) = log.readv(0, 0, 200 * 1024).unwrap();
        assert_eq!(segment, 80);
        assert_eq!(offset, 9);
        assert_eq!(ids.len(), 90);
        for i in 0..ids.len() {
            assert_eq!(i as u64 + 1, ids[i])
        }
        assert_eq!(done, true);
        assert_eq!(total_size, 90 * 1024);
    }
}
