use crate::{Cursor, Offset};

use super::Storage;
use std::io;

pub(crate) struct Segment<T> {
    /// Holds the actual segment.
    pub(crate) data: Vec<T>,
    total_size: u64,
    /// The absolute offset at which the `inner` starts at. All reads will return the absolute
    /// offset as the offset of the cursor.
    ///
    /// **NOTE**: this offset is re-generated on each run of the commit log.
    pub(crate) absolute_offset: u64,
}

pub(crate) enum SegmentPosition {
    /// When the returned absolute offset exists within the current segment
    Next(u64),
    /// When the the returned absolute offset does not exist within the current segment, but
    /// instead is 1 beyond the highest absolute offset in this segment, meant for use with next
    /// segment if any exists.
    Done(u64),
}

impl<T> Segment<T>
where
    T: Storage + Clone,
{
    pub(crate) fn with_offset(absolute_offset: u64) -> Self {
        Self {
            data: Vec::with_capacity(1024),
            absolute_offset,
            total_size: 0,
        }
    }
    pub(crate) fn new() -> Self {
        Self {
            data: Vec::with_capacity(1024),
            absolute_offset: 0,
            total_size: 0,
        }
    }

    #[inline]
    pub(crate) fn next_offset(&self) -> u64 {
        self.absolute_offset + self.len()
    }

    /// Push a new `T` in the segment.
    #[inline]
    pub(crate) fn push(&mut self, inner_type: T) {
        self.total_size += inner_type.size() as u64;
        self.data.push(inner_type);
    }

    #[inline]
    /// Takes in the abosolute index to start reading from. Internally handles the conversion from
    /// relative offset to absolute offset and vice-versa.
    pub(crate) fn readv(
        &self,
        cursor: Cursor,
        len: u64,
        out: &mut Vec<(T, Offset)>,
    ) -> io::Result<SegmentPosition> {
        // This substraction can never overflow as checking of offset happens at
        // `CommitLog::readv`.
        let idx = cursor.1 - self.absolute_offset;

        let mut ret: Option<u64>;

        if idx >= self.len() {
            ret = None;
        } else {
            let mut limit = idx + len;

            ret = Some(limit);

            if limit >= self.len() {
                ret = None;
                limit = self.len();
            }
            let offsets = std::iter::repeat(cursor.0).zip(cursor.1..cursor.1 + limit);
            let o = self.data[idx as usize..limit as usize]
                .iter()
                .cloned()
                .zip(offsets);
            out.extend(o);
        }

        match ret {
            Some(relative_offset) => Ok(SegmentPosition::Next(
                self.absolute_offset + relative_offset,
            )),
            None => Ok(SegmentPosition::Done(self.next_offset())),
        }
    }

    /// Get the number of `T` in the segment.
    #[inline]
    pub(crate) fn len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Get the total size in bytes of the segment.
    #[inline]
    pub(crate) fn size(&self) -> u64 {
        self.total_size
    }

    #[inline]
    pub fn last(&self) -> Option<T> {
        self.data.last().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn segment_works_for_bytes() {
        let mut mem_segment: Segment<Bytes> = Segment::new();
        let test_byte = Bytes::from_static(b"test1");
        mem_segment.push(test_byte.clone());
        assert_eq!(mem_segment.len(), 1);
        assert_eq!(mem_segment.last().unwrap(), test_byte);
    }

    #[test]
    fn readv_works_for_bytes() {
        let mut segment: Segment<Bytes> = Segment::new();
        segment.push(Bytes::from_static(b"test1"));
        segment.push(Bytes::from_static(b"test2"));
        segment.push(Bytes::from_static(b"test3"));
        segment.push(Bytes::from_static(b"test4"));
        segment.push(Bytes::from_static(b"test5"));
        segment.push(Bytes::from_static(b"test6"));
        segment.push(Bytes::from_static(b"test7"));
        segment.push(Bytes::from_static(b"test8"));
        segment.push(Bytes::from_static(b"test9"));
        assert_eq!(segment.len(), 9);

        let mut out: Vec<(Bytes, Offset)> = Vec::new();
        let _ = segment.readv((0, 0), 2, &mut out).unwrap();
        assert_eq!(
            out,
            vec![
                (Bytes::from_static(b"test1"), (0, 0)),
                (Bytes::from_static(b"test2"), (0, 1))
            ]
        );
    }

    #[test]
    fn readv_works_for_vec_of_u8() {
        let mut segment: Segment<Vec<u8>> = Segment::new();
        segment.push(vec![1u8]);
        segment.push(vec![2u8]);
        segment.push(vec![3u8]);
        segment.push(vec![4u8]);
        segment.push(vec![5u8]);
        segment.push(vec![6u8]);
        segment.push(vec![7u8]);
        segment.push(vec![8u8]);
        segment.push(vec![9u8]);
        assert_eq!(segment.len(), 9);

        let mut out: Vec<(Vec<u8>, Offset)> = Vec::new();
        let _ = segment.readv((0, 0), 2, &mut out).unwrap();
        assert_eq!(out, vec![(vec![1u8], (0, 0)), (vec![2u8], (0, 1))]);
    }
}
