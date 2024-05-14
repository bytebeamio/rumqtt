use crate::Offset;
use std::{collections::VecDeque, io};

mod segment;
pub mod utils;

use segment::{Segment, SegmentPosition};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Position {
    Next { start: (u64, u64), end: (u64, u64) },
    Done { start: (u64, u64), end: (u64, u64) },
}

pub trait Storage {
    fn size(&self) -> usize;
}

/// There are 2 limits which are enforced:
/// - limit on size of each segment created by this log in bytes
/// - limit on number of segments in memory
///
/// When the active_segment is filled up, we move it to memory segments and empty it for new logs.
/// When the limit on the number of memory segments is reached, we remove the oldest segment from
/// memory segments.
///
/// This shifting of segments happens everytime the limit on the size of a segment exceeds the
/// limit. Note that the size of a segment might go beyond the limit if the single last log was put
/// at the offset which is within the limit but the logs size was large enough to be beyond the
/// limit. Only when another log is appended will we flush the active segment onto memory segments.
///
/// ### Invariants
/// - The active segment should have index `tail`.
/// - Segments throughout should be contiguous in their indices.
/// - The total size in bytes for each segment in memory should not increase beyond the
///   max_segment_size by more than the overflowing bytes of the last packet.
///
/// ### Seperation of implementation
///    - `index` & `segment` - everything directly related to files, no bounds check except when
///      bounds exceed file's existing size.
///    - `chunk` - abstraction to deal with index and segment combined. Basically we only need
///      stuff from segment file, and thus we hide away the index file under this abstraction.
///    - `segment` - abstracts away the memory segments for ease of access.
pub struct CommitLog<T> {
    /// The index at which segments start.
    head: u64,
    /// The index at which the current active segment is, and also marks the last valid segment as
    /// well as the last segment in memory.
    tail: u64,
    /// Maximum size of any segment in memory in bytes.
    max_segment_size: usize,
    /// Maximum number of segments in memory, apart from the active segment.
    max_mem_segments: usize,
    /// Total size of active segment, used for enforcing the contraints.
    segments: VecDeque<Segment<T>>,
}

impl<T> CommitLog<T>
where
    T: Storage + Clone,
{
    /// Create a new `CommitLog` with the given contraints. If `max_mem_segments` is 0, then only
    /// the active segment is maintained.
    pub fn new(max_segment_size: usize, max_mem_segments: usize) -> io::Result<Self> {
        if max_segment_size < 1024 {
            panic!("given max_segment_size {max_segment_size} bytes < 1KB");
        }

        if max_mem_segments < 1 {
            panic!("at least 1 segment needs to exist in memory else what's the point of log");
        }

        let mut segments = VecDeque::with_capacity(max_mem_segments);
        segments.push_back(Segment::new());

        Ok(Self {
            head: 0,
            tail: 0,
            max_segment_size,
            max_mem_segments,
            segments,
        })
    }

    #[inline]
    pub fn next_offset(&self) -> (u64, u64) {
        // `unwrap` fine as we are guaranteed that active segment always exist and is at the end
        (self.tail, self.active_segment().next_offset())
    }

    #[inline]
    pub fn _head_and_tail(&self) -> (u64, u64) {
        (self.head, self.tail)
    }

    #[inline]
    pub fn memory_segments_count(&self) -> usize {
        self.segments.len()
    }

    /// Size of data in all the segments
    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        let mut size = 0;
        for segment in self.segments.iter() {
            size += segment.size();
        }
        size
    }

    /// Number of segments
    #[allow(dead_code)]
    #[inline]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Number of packets
    #[inline]
    #[allow(dead_code)]
    pub fn entries(&self) -> u64 {
        self.active_segment().next_offset()
    }

    #[inline]
    fn active_segment(&self) -> &Segment<T> {
        self.segments.back().unwrap()
    }

    #[inline]
    fn active_segment_mut(&mut self) -> &mut Segment<T> {
        self.segments.back_mut().unwrap()
    }

    /// Append a new [`T`] to the active segment.
    #[inline]
    pub fn append(&mut self, message: T) -> (u64, u64) {
        self.apply_retention();
        let active_segment = self.active_segment_mut();
        active_segment.push(message);
        let absolute_offset = self.active_segment().next_offset();
        (self.tail, absolute_offset)
    }

    fn apply_retention(&mut self) {
        if self.active_segment().size() >= self.max_segment_size as u64 {
            // Read absolute_offset before applying memory retention, in case there is only 1
            // segment allowed.
            let absolute_offset = self.active_segment().next_offset();
            // If active segment is full and segments are full, apply retention policy.
            if self.memory_segments_count() >= self.max_mem_segments {
                self.segments.pop_front();
                self.head += 1;
            }

            // Pushing a new segment into segments and updating the tail automatically changes
            // the active segment to a new empty one.
            self.segments
                .push_back(Segment::with_offset(absolute_offset));
            self.tail += 1;
        }
    }

    #[inline]
    pub fn last(&self) -> Option<T> {
        self.active_segment().last()
    }

    /// Read `len` Ts at once. More efficient than reading 1 at a time. Returns
    /// the next offset to read data from. The Position::start returned need not
    /// be a valid index if the start given is not valid either.
    pub fn readv(
        &self,
        mut start: (u64, u64),
        mut len: u64,
        out: &mut Vec<(T, Offset)>,
    ) -> io::Result<Position> {
        let mut cursor = start;
        let _orig_cursor = cursor;

        if cursor.0 > self.tail {
            return Ok(Position::Done { start, end: start });
        }

        if cursor.0 < self.head {
            let head_absolute_offset = self.segments.front().unwrap().absolute_offset;
            warn!(
                "given index {} less than head {}, jumping to head",
                cursor.0, head_absolute_offset
            );
            cursor = (self.head, head_absolute_offset);
            start = cursor;
        }

        let mut idx = (cursor.0 - self.head) as usize;
        let mut curr_segment = &self.segments[idx];

        if curr_segment.absolute_offset > cursor.1 {
            warn!(
                "offset specified {} if less than actual {}, jumping",
                cursor.1, curr_segment.absolute_offset
            );
            start.1 = curr_segment.absolute_offset;
            cursor.1 = curr_segment.absolute_offset;
        }

        while cursor.0 < self.tail {
            // `Segment::readv` handles the conversion from absolute index to relative
            // index and it returns the absolute offset.
            // absolute cursor not to be confused with absolute offset
            match curr_segment.readv(cursor, len, out)? {
                // an offset returned -> we didn't read till end -> len fulfilled -> return
                SegmentPosition::Next(offset) => {
                    return Ok(Position::Next {
                        start,
                        end: (cursor.0, offset),
                    });
                }
                // no offset returned -> we reached end
                // if len unfulfilled -> try next segment with remaining length
                SegmentPosition::Done(next_offset) => {
                    // This condition is needed in case cursor.1 > 0 (when the user provies cursor.1
                    // beyond segment's last offset which can happen due to next readv offset
                    // being off by 1 before jumping to next segment or while manually reading
                    // from a particular cursor). In such case, the no. of read data points is
                    // 0 and hence we don't decrement len.
                    if next_offset >= cursor.1 {
                        len -= next_offset - cursor.1;
                    }
                    cursor = (cursor.0 + 1, next_offset);
                }
            }

            if len == 0 {
                // debug!("start: {:?}, end: ({}, {})", orig_cursor, cursor.0, cursor.1 - 1);
                return Ok(Position::Next { start, end: cursor });
            }

            idx += 1;
            curr_segment = &self.segments[idx];
        }

        if curr_segment.next_offset() <= cursor.1 {
            return Ok(Position::Done { start, end: cursor });
        }

        // We need to read seperately from active segment because if `None` is returned for active
        // segment's `readv`, then we should return `None` as well as it is not possible to read
        // further, whereas for older segments we simply jump on to the new one to read more.

        match curr_segment.readv(cursor, len, out)? {
            SegmentPosition::Next(v) => {
                // debug!("start: {:?}, end: ({}, {})", orig_cursor, cursor.0, cursor.1 + v - 1);
                Ok(Position::Next {
                    start,
                    end: (cursor.0, v),
                })
            }
            SegmentPosition::Done(absolute_offset) => {
                // debug!("start: {:?}, end: ({}, {}) done", orig_cursor, cursor.0, absolute_offset);
                Ok(Position::Done {
                    start,
                    end: (cursor.0, absolute_offset),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Position::*, *};
    use bytes::Bytes;
    use pretty_assertions::assert_eq;

    fn random_payload(id: u8, size: u64) -> Bytes {
        Bytes::from(vec![id; size as usize])
    }

    fn verify(expected_id: usize, expected_size: u64, out: (Bytes, Offset)) {
        let expected = Bytes::from(vec![expected_id as u8; expected_size as usize]);
        // dbg!(expected_id, &expected);
        assert_eq!(out.0, expected);
    }

    #[test]
    fn reading_at_invalid_cursor_returns_none() {
        // 1 as active only
        let log: CommitLog<Bytes> = CommitLog::new(1024, 1).unwrap();
        let mut out = Vec::new();

        assert_eq!(log.head, 0);
        assert_eq!(log.tail, 0);
        assert_eq!(
            log.readv((0, 1), 2, &mut out).unwrap(),
            Done {
                start: (0, 1),
                end: (0, 1)
            }
        );
        assert_eq!(
            log.readv((100, 1), 2, &mut out).unwrap(),
            Done {
                start: (100, 1),
                end: (100, 1)
            }
        );
    }

    #[test]
    fn inmemory_appends_and_retention_policy_works() {
        let max_segment_size = 1024 * 100; // 100K
        let packet_size = 1024;
        // 1 as active 1 as inactive but in mem
        let mut log: CommitLog<Bytes> = CommitLog::new(max_segment_size, 2).unwrap();

        // Fill the active segment
        for i in 0..100 {
            let offset = log.append(random_payload(i as u8, packet_size));
            assert_eq!(offset, (0, i as u64 + 1))
        }
        assert_eq!(log.size(), max_segment_size as u64);
        assert_eq!(log.head, 0);
        assert_eq!(log.tail, 0);
        assert_eq!(log.len(), 1);

        // Append more data to trigger new segment creation
        log.append(random_payload(100, packet_size));
        assert_eq!(log.head, 0);
        assert_eq!(log.tail, 1);
        assert_eq!(log.len(), 2);

        // Fill the rest of new active segment
        for (i, v) in (101..200).enumerate() {
            let offset = log.append(random_payload(v, packet_size));
            assert_eq!(offset, (1, i as u64 + 102))
        }
        assert_eq!(log.head, 0);
        assert_eq!(log.tail, 1);
        assert_eq!(log.len(), 2);

        // Append more data to trigger new segment creation and retention policy
        log.append(random_payload(200, packet_size));
        assert_eq!(log.head, 1);
        assert_eq!(log.tail, 2);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn active_segment_appends_and_reads_works() {
        let max_segment_size = 1024 * 100; // 100K
        let packet_size: u64 = 1024;
        // 1 as active only
        let mut log = CommitLog::new(max_segment_size, 1).unwrap();

        for i in 0..10 {
            log.append(random_payload(i, packet_size));
        }

        assert_eq!(log.active_segment().len(), 10);
        assert_eq!(log.active_segment().size(), packet_size * 10);

        // Read one by one
        let mut out = Vec::new();
        for i in 0..10 {
            let offset = i as u64;
            let next = log.readv((0, offset), 1, &mut out).unwrap();
            let data = out.pop().unwrap();
            verify(i, packet_size, data);

            if i == 9 {
                assert_eq!(
                    next,
                    Done {
                        start: (0, 9),
                        end: (0, 10)
                    }
                );
                continue;
            }

            assert_eq!(
                next,
                Next {
                    start: (0, i as u64),
                    end: (0, i as u64 + 1)
                }
            );
        }

        // Read in bulk 1. Trying to read less than appended
        let mut out = Vec::new();
        let next = log.readv((0, 0), 5, &mut out).unwrap();
        assert_eq!(out.len(), 5);
        out.into_iter()
            .enumerate()
            .for_each(|(i, v)| verify(i, packet_size, v));
        assert_eq!(
            next,
            Next {
                start: (0, 0),
                end: (0, 5)
            }
        );

        // Read in bulk 2. Trying to read exactly appended elements
        let mut out = Vec::new();
        let next = log.readv((0, 0), 10, &mut out).unwrap();
        assert_eq!(out.len(), 10);
        out.into_iter()
            .enumerate()
            .for_each(|(i, v)| verify(i, packet_size, v));
        assert_eq!(
            next,
            Done {
                start: (0, 0),
                end: (0, 10)
            }
        );

        // Read in bulk 3. Trying to read greater than appended
        let mut out = Vec::new();
        let next = log.readv((0, 0), 20, &mut out).unwrap();
        assert_eq!(out.len(), 10);
        out.into_iter()
            .enumerate()
            .for_each(|(i, v)| verify(i, packet_size, v));
        assert_eq!(
            next,
            Done {
                start: (0, 0),
                end: (0, 10)
            }
        );

        // Read in bulk 4. Trying to read greater than appended but from middle of the segment
        let mut out = Vec::new();
        let next = log.readv((0, 5), 20, &mut out).unwrap();
        assert_eq!(out.len(), 5);
        out.into_iter()
            .enumerate()
            .for_each(|(i, v)| verify(i + 5, packet_size, v));
        assert_eq!(
            next,
            Done {
                start: (0, 5),
                end: (0, 10)
            }
        );

        // Read again after after appending again
        let mut out = Vec::new();
        let next = log.readv((0, 10), 20, &mut out).unwrap();
        assert_eq!(
            next,
            Done {
                start: (0, 10),
                end: (0, 10)
            }
        );
        for i in 10..20 {
            log.append(random_payload(i, packet_size));
        }
        let next = log.readv((0, 10), 20, &mut out).unwrap();
        assert_eq!(out.len(), 10);
        out.into_iter()
            .enumerate()
            .for_each(|(i, v)| verify(i + 10, packet_size, v));
        assert_eq!(
            next,
            Done {
                start: (0, 10),
                end: (0, 20)
            }
        );
    }

    #[test]
    fn read_switch_from_active_to_inactive_to_active_segment_works() {
        let max_segment_size = 1024 * 100; // 100K
        let packet_size: u64 = 1024;
        // 1 as active, 3 as inactive but in mem
        let mut log = CommitLog::new(max_segment_size, 4).unwrap();

        // Fill active segment
        for i in 0..100 {
            log.append(random_payload(i, packet_size));
        }
        assert_eq!(log.head, 0);
        assert_eq!(log.tail, 0);
        assert_eq!(log.active_segment().len(), 100);
        assert_eq!(log.active_segment().size(), packet_size * 100);

        // Read partially from active segment
        let mut out = Vec::new();
        let next = log.readv((0, 0), 50, &mut out).unwrap();
        assert_eq!(out.len(), 50);
        assert_eq!(
            next,
            Next {
                start: (0, 0),
                end: (0, 50)
            }
        );

        // Fill with data worth 2 more segments. Active segment will change
        let mut out = Vec::new();
        for i in 0..200 {
            log.append(random_payload(i, packet_size));
        }
        assert_eq!(log.head, 0);
        assert_eq!(log.tail, 2);

        // Read from previous next
        let next = log.readv((0, 50), 50, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (0, 50),
                end: (1, 100)
            }
        );

        let next = log.readv((1, 100), 100, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (1, 100),
                end: (2, 200)
            }
        );

        let next = log.readv((2, 200), 100, &mut out).unwrap();
        assert_eq!(
            next,
            Done {
                start: (2, 200),
                end: (2, 300)
            }
        );
    }

    #[test]
    fn read_with_jumps_works() {
        let max_segment_size = 1024 * 10; // 10K
        let packet_size: u64 = 1024;
        // 1 as active, 4 as inactive but in mem
        let mut log = CommitLog::new(max_segment_size, 5).unwrap();

        // Fill active segment + 3 more memory segments
        for i in 0..40 {
            log.append(random_payload(i, packet_size));
        }

        // One big jump
        let mut out = Vec::new();
        let next = log.readv((0, 0), 35, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (0, 0),
                end: (3, 35)
            }
        );

        // Each readv less than segment count. Segment count = 10. Readv = 5
        let mut out = Vec::new();
        let next = log.readv((0, 0), 5, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (0, 0),
                end: (0, 5)
            }
        );
        let next = log.readv((3, 5), 5, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (3, 30),
                end: (3, 35)
            }
        );
        let next = log.readv((3, 40), 5, &mut out).unwrap();
        assert_eq!(
            next,
            Done {
                start: (3, 40),
                end: (3, 40)
            }
        );
        let next = log.readv((4, 40), 5, &mut out).unwrap();
        assert_eq!(
            next,
            Done {
                start: (4, 40),
                end: (4, 40)
            }
        );
        let next = log.readv((4, 41), 5, &mut out).unwrap();
        assert_eq!(
            next,
            Done {
                start: (4, 41),
                end: (4, 41)
            }
        );

        // Each readv greater than segment count. Segment count = 10. Readv = 5
        let mut out = Vec::new();
        let next = log.readv((0, 0), 15, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (0, 0),
                end: (1, 15)
            }
        );
        let next = log.readv((1, 15), 15, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (1, 15),
                end: (3, 30)
            }
        );
        let next = log.readv((3, 30), 10, &mut out).unwrap();
        assert_eq!(
            next,
            Done {
                start: (3, 30),
                end: (3, 40)
            }
        );
    }

    #[test]
    fn read_jump_from_deleted_segment_works() {
        let max_segment_size = 1024 * 10; // 10K
        let packet_size: u64 = 1024;
        // 1 as active, 9 as inactive but in mem
        let mut log = CommitLog::new(max_segment_size, 10).unwrap();

        // Fill all 10 in memory segments
        for i in 0..100 {
            log.append(random_payload(i, packet_size));
        }
        assert_eq!(log.head, 0);
        assert_eq!(log.tail, 9);

        let mut out = Vec::new();
        let next = log.readv((0, 0), 5, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (0, 0),
                end: (0, 5)
            }
        );

        // Fill 10 more inmemory segment pushing previous into retention policy
        for i in 0..100 {
            log.append(random_payload(i, packet_size));
        }
        assert_eq!(log.head, 10);
        assert_eq!(log.tail, 19);

        let next = log.readv((0, 0), 5, &mut out).unwrap();
        assert_eq!(
            next,
            Next {
                start: (10, 100),
                end: (10, 105)
            }
        );
    }
}
