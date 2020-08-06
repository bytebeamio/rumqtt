mod segment;

use bytes::Bytes;
use intmap::IntMap;

use segment::Segment;
use std::collections::VecDeque;


pub struct Log {
    /// Offset of the last appended record
    next_offset: u64,
    /// Maximum size of a segment
    max_segment_size: usize,
    /// Maximum number of segments
    max_segments: usize,
    /// Current active chunk to append
    active_chunk: Segment,
    /// All the segments in a ringbuffer
    segments: IntMap<Segment>,
}

impl Log {
    /// Creates a new Log which is a collection of segments
    /// Segments backed by a ring buffer. Indexes sequentially increase
    /// with out allocation
    pub fn new(max_segment_size: usize, max_segments: usize) -> Log {
        if max_segment_size < 1024 {
            panic!("size should be at least 1KB")
        }

        let mut segments = VecDeque::with_capacity(max_segments);
        let segment = Segment::new(0);
        segments.push_back(segment);

        Log {
            next_offset: 0,
            max_segment_size,
            max_segments,
            segments,
            active_chunk: 0,
        }
    }

    /// Appends this record to the tail and returns the offset of this append.
    /// When the current segment is full, this also create a new segment and
    /// writes the record to it. This function also handles retention by removing
    /// head segment
    pub fn append(&mut self, record: Bytes) -> u64 {
        let mut active_chunk = self.segments.back_mut().unwrap();

        if active_chunk.size() >= self.max_segment_size {
            let segment = Segment::new(0);
            self.segments.push_back(segment);

            if self.segments.len() > self.max_segments {
                self.segments.pop_front();
            }

            active_chunk = self.segments.back_mut().unwrap();
            self.next_offset = 0;
        }

        active_chunk.append(record);

        let last_offset = self.next_offset;
        self.next_offset += 1;
        last_offset
    }

    /// Read a record from correct segment
    /// Returns data, next base offset and relative offset
    pub fn read(&mut self, segment_index: usize, offset: usize) -> Option<Bytes> {
        let segment = match self.segments.get_mut(segment_index) {
            Some(segment) => segment,
            None => return None
        };

        segment.read(offset)
    }

    /// Reads multiple packets from the storage and return base offset and relative offset of the
    /// Returns base offset, relative offset of the last record along with number of messages and count
    /// Goes to next segment when relative off set crosses boundary
    pub fn readv(&mut self, segment_index: usize, segment_offset: usize) -> (usize, usize, Vec<Bytes>) {
        // return everything in this segment if there is a next segment
        if self.segments.len() >= segment_index + 1 {
            let segment = self.segments.get(segment_index).unwrap();
            let out = segment.file.get(segment_offset..).unwrap().to_vec();
            let segment = segment_index;
            return (segment, segment_offset, out)
        }

        let out = self.segments.get(segment_index).unwrap().readv(segment_offset);
        (segment_index, segment_offset, out)
    }
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

    /*
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

     */
}
