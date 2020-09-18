mod segment;

use bytes::Bytes;
use fnv::FnvHashMap;

use segment::Segment;
use std::mem;

/// Log is an inmemory commitlog (per topic) which splits data in segments.
/// It drops the oldest segment when retention policies are crossed.
/// Each segment is identified by base offset and a new segment is created
/// when ever current segment crosses storage limit
pub struct Log {
    /// Offset of the last appended record
    head_offset: u64,
    /// Maximum size of a segment
    max_segment_size: usize,
    /// Maximum number of segments
    max_segments: usize,
    /// Current active chunk to append
    active_segment: Segment,
    /// All the segments in a ringbuffer
    segments: FnvHashMap<u64, Segment>,
}

impl Log {
    /// Create a new log
    pub fn new(max_segment_size: usize, max_segments: usize) -> Log {
        if max_segment_size < 1024 {
            panic!("size should be at least 1KB")
        }

        Log {
            head_offset: 0,
            max_segment_size,
            max_segments,
            segments: FnvHashMap::default(),
            active_segment: Segment::new(0),
        }
    }

    /// Appends this record to the tail and returns the offset of this append.
    /// When the current segment is full, this also create a new segment and
    /// writes the record to it.
    /// This function also handles retention by removing head segment
    pub fn append(&mut self, record: Bytes) -> (u64, u64) {
        if self.active_segment.size() >= self.max_segment_size {
            let next_offset = self.active_segment.base_offset() + self.active_segment.len() as u64;
            let last_active = mem::replace(&mut self.active_segment, Segment::new(next_offset));
            self.segments.insert(last_active.base_offset(), last_active);

            // if backlog + active segment count is greater than max segments,
            // delete first segment and update head
            if self.segments.len() + 1 > self.max_segments {
                if let Some(segment) = self.segments.remove(&self.head_offset) {
                    self.head_offset = segment.base_offset() + segment.len() as u64;
                }
            }
        }

        let base_offset = self.active_segment.base_offset();
        (base_offset, self.active_segment.append(record))
    }

    pub fn last_offset(&self) -> (u64, u64) {
        let base_offset = self.active_segment.base_offset();
        // Segment will always have at least 1 element as it's lazily
        // created with append. Hence we can ignore -ve overflow
        let relative_offset = base_offset + self.active_segment.len() as u64 - 1;

        (base_offset, relative_offset)
    }

    pub fn next_offset(&self) -> (u64, u64) {
        let base_offset = self.active_segment.base_offset();
        // Segment will always have at least 1 element as it's lazily
        // created with append. Hence we can ignore -ve overflow
        let relative_offset = base_offset + self.active_segment.len() as u64;

        (base_offset, relative_offset)
    }

    /// Read a record from correct segment
    pub fn read(&mut self, base_offset: u64, offset: usize) -> Option<Bytes> {
        if base_offset == self.active_segment.base_offset() {
            return self.active_segment.read(offset as usize);
        }

        match self.segments.get_mut(&base_offset) {
            Some(segment) => segment.read(offset),
            None => None,
        }
    }

    /// Reads multiple packets from the storage and return base offset and relative
    /// offset of the last segment. Done status is used by the caller to jump to next segment
    /// Returns base offset, offset of the last record along will records batch
    /// **Note**: Base offset is used to be able to pull directly from correct segment instead of
    /// using an explicit index to identify correct segment. Kafka clients just use absolute
    /// offset in request. Upper layers should somehow translate absolute offet to base offset
    /// **Note**: This method also returns full segment data when requested data is not of active
    /// segment. Set your max_segment size keeping tail latencies of all the concurrent
    /// connections mind (some runtimes support internal preemption using await points
    /// where this might not be a problem)
    /// **Note**: When data of deleted segment is asked, returns data of the current head
    pub fn readv(
        &mut self,
        mut base_offset: u64,
        mut offset: u64,
        max_count: usize,
    ) -> (Option<u64>, u64, u64, Vec<Bytes>) {
        // TODO Fix usize to u64 conversions

        // jump to head if the caller is trying to read deleted segment
        if base_offset < self.head_offset {
            warn!("Trying to read a deleted segment. Jumping");
            base_offset = self.head_offset;
            offset = self.head_offset;
        }

        // read from active segment if base offset matches active segment's base offset
        if base_offset == self.active_segment.base_offset() {
            let relative_offset = (offset - base_offset) as usize;
            let out = self.active_segment.readv(relative_offset, max_count);
            let last_record_offset = offset + out.len() as u64 - 1;
            return (
                None,
                self.active_segment.base_offset(),
                last_record_offset,
                out,
            );
        }

        // read from backlog segments
        if let Some(segment) = self.segments.get(&base_offset) {
            let relative_offset = (offset - base_offset) as usize;
            let out = segment.readv(relative_offset, max_count);

            return if out.len() > 0 {
                let last_record_offset = offset + out.len() as u64 - 1;
                let next_segment_offset = segment.base_offset() + segment.len() as u64;
                (
                    Some(next_segment_offset),
                    segment.base_offset(),
                    last_record_offset,
                    out,
                )
            } else {
                let out = self.active_segment.readv(0, max_count);
                let last_record_offset = offset + out.len() as u64 - 1;
                (
                    None,
                    self.active_segment.base_offset(),
                    last_record_offset,
                    out,
                )
            };
        }

        (None, base_offset, offset, Vec::new())
    }
}

#[cfg(test)]
mod test {
    use super::Log;
    use bytes::Bytes;
    use pretty_assertions::assert_eq;

    #[test]
    fn append_creates_and_deletes_segments_correctly() {
        let mut log = Log::new(10 * 1024, 10);

        // 200 1K iterations. 10 1K records per file. 20 files ignoring deletes.
        // segments: 0.segment, 10.segment .... 190.segment
        // considering deletes: 100.segment .. 190.segment
        for i in 0..200 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // Semi fill 200.segment. Deletes 100.segment
        // considering deletes: 110.segment .. 190.segment
        for i in 200..205 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        let data = log.read(90, 0);
        assert!(data.is_none());

        // considering: 100.segment (100-109) .. 190.segment (190-199)
        // read segment with base offset 110
        let base_offset = 110;
        for i in 0..10 {
            let data = log.read(base_offset, i).unwrap();
            let d = base_offset as u8 + i as u8;
            assert_eq!(data[0], d);
        }

        // read segment with base offset 190 (1 last segment before
        // semi filled segment)
        let base_offset = 190;
        for i in 0..10 {
            let data = log.read(base_offset, i).unwrap();
            let d = base_offset as u8 + i as u8;
            assert_eq!(data[0], d);
        }

        // read 200.segment which is semi filled with 5 records
        let base_offset = 200;
        for i in 0..5 {
            let data = log.read(base_offset, i).unwrap();
            let d = base_offset as u8 + i as u8;
            assert_eq!(data[0], d);
        }

        let data = log.read(base_offset, 5);
        assert!(data.is_none());
    }

    #[test]
    fn vectored_read_works_as_expected() {
        let mut log = Log::new(10 * 1024, 10);

        // 90 1K iterations. 10 files ignoring deletes.
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 80.segment (80 - 89)
        // 10K per segment = 10 records per segment
        for i in 0..90 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // Read a segment from start. This returns full segment
        let (jump, base_offset, last_offset, data) = log.readv(0, 0, 10);
        assert_eq!(data.len(), 10);
        assert_eq!(base_offset, 0);
        assert_eq!(last_offset, 9);
        assert_eq!(data[base_offset as usize][0], 0);
        assert_eq!(data[last_offset as usize][0], 9);
        assert_eq!(jump, Some(10));

        // Read 50.segment offset 0
        let data = log.read(50, 0).unwrap();
        assert_eq!(data[0], 50);

        // Read a segment from the middle. This returns all the remaining elements
        let (jump, base_offset, last_offset, data) = log.readv(10, 15, 10);
        assert_eq!(data.len(), 5);
        assert_eq!(base_offset, 10);
        assert_eq!(last_offset, 19);
        assert_eq!(data[0][0], 15);
        assert_eq!(data[data.len() - 1][0], 19);
        assert_eq!(jump, Some(20));

        // Read a segment from scratch and max count crosses boundary
        let (_, _, _, data) = log.readv(10, 10, 100);
        assert_eq!(data.len(), 10);

        // Read a segment from scratch and max count less than boundary
        let (_, base_offset, last_offset, data) = log.readv(10, 10, 5);
        assert_eq!(data.len(), 5);
        assert_eq!(base_offset, 10);
        assert_eq!(last_offset, 14);

        // Read a segment from middle and max count crosses boundary
        let (_, _, _, data) = log.readv(10, 15, 100);
        assert_eq!(data.len(), 5);

        // Read a segment from middle and max count less than boundary
        let (_, base_offset, last_offset, data) = log.readv(10, 15, 2);
        assert_eq!(data.len(), 2);
        assert_eq!(base_offset, 10);
        assert_eq!(last_offset, 16);
    }

    #[test]
    fn vectored_reads_from_active_segment_works_as_expected() {
        let mut log = Log::new(10 * 1024, 10);

        // 200 1K iterations. 10 1K records per file. 20 files ignoring deletes.
        // segments: 0.segment, 10.segment .... 190.segment
        // considering deletes: 100.segment .. 190.segment
        for i in 0..200 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // read active segment
        let (jump, segment, offset, data) = log.readv(190, 190, 10);
        assert_eq!(data.len(), 10);
        assert_eq!(segment, 190);
        assert_eq!(offset, 199);
        assert!(jump.is_none());
    }

    #[test]
    fn vectored_reads_from_active_segment_resumes_after_empty_reads_correctly() {
        let mut log = Log::new(10 * 1024, 10);

        // 85 1K iterations. 10 files
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 80.segment (80 - 84)
        // 10 records per segment (1K each)
        // segment 80 is semi filled
        for i in 0..85 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // read active segment
        let (jump, segment, offset, data) = log.readv(80, 80, 10);
        assert_eq!(data.len(), 5);
        assert_eq!(segment, 80);
        assert_eq!(offset, 84);
        assert!(jump.is_none());

        // fill active segment more
        for i in 85..90 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // read active segment
        let (jump, segment, offset, data) = log.readv(segment, offset + 1, 10);
        assert_eq!(data.len(), 5);
        assert_eq!(segment, 80);
        assert_eq!(offset, 89);
        assert!(jump.is_none());
    }

    #[test]
    fn last_active_segment_read_jumps_to_current_active_segment_read_correctly() {
        let mut log = Log::new(10 * 1024, 10);

        // 90 1K iterations. 9 files ignoring deletes.
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 80.segment (80 - 89)
        // 10K per segment = 10 records per segment
        for i in 0..90 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // read active segment
        let (jump, segment, offset, data) = log.readv(80, 80, 10);
        assert_eq!(data.len(), 10);
        assert_eq!(segment, 80);
        assert_eq!(offset, 89);
        assert!(jump.is_none());

        // append more which also changes active segment
        for i in 90..100 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // read from the next offset of previous active segment
        let (jump, segment, offset, data) = log.readv(segment, offset + 1, 10);
        assert_eq!(data.len(), 10);
        assert_eq!(segment, 90);
        assert_eq!(offset, 99);
        assert!(jump.is_none());
    }

    #[test]
    fn vectored_reads_reports_jumps_at_boundary() {
        let mut log = Log::new(10 * 1024, 10);

        // 200 1K iterations. 10 1K records per file. 20 files ignoring deletes.
        // segments: 0.segment, 10.segment .... 190.segment
        // considering deletes: 100.segment .. 190.segment
        for i in 0..200 {
            let payload = vec![i; 1024];
            let payload = Bytes::from(payload);
            log.append(payload);
        }

        // Read 15K. Crosses boundaries of the segment and offset will be in the middle of 2nd segment
        let (jump, segment, offset, _data) = log.readv(0, 0, 10);
        assert_eq!(segment, 100);
        assert_eq!(offset, 109);
        assert!(jump.is_some());
    }
}
