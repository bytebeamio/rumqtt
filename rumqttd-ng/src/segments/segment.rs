use std::io;

use bytes::Bytes;

use super::memory::MemorySegment;

pub(crate) struct Segment {
    /// Holds the actual segment.
    pub(crate) inner: SegmentType,
    /// The absolute offset at which the `inner` starts at. All reads will return the absolute
    /// offset as the offset of the cursor.
    ///
    /// **NOTE**: this offset is re-generated on each run of the commit log.
    pub(crate) absolute_offset: u64,
}

#[derive(Debug)]
pub(crate) enum SegmentType {
    Memory(MemorySegment),
}

pub(crate) enum SegmentPosition {
    /// When the returned absolute offset exists within the current segment
    Next(u64),
    /// When the the returned absolute offset does not exist within the current segment, but
    /// instead is 1 beyond the highest absolute offset in this segment, meant for use with next
    /// segment if exists
    Done(u64),
}

impl Segment {
    #[inline]
    pub(crate) fn next_absolute_offset(&self) -> u64 {
        self.len() + self.absolute_offset
    }

    #[inline]
    /// Takes in the abosolute index to start reading from. Internally handles the conversion from
    /// relative offset to absolute offset and vice-versa.
    pub(crate) fn readv(
        &self,
        absolute_index: u64,
        len: u64,
        out: &mut Vec<Bytes>,
    ) -> io::Result<SegmentPosition> {
        // this substraction can never overflow as checking of offset happens at
        // `CommitLog::readv`.
        let idx = absolute_index - self.absolute_offset;

        match &self.inner {
            SegmentType::Memory(segment) => match segment.readv(idx, len, out) {
                Some(relative_offset) => Ok(SegmentPosition::Next(
                    self.absolute_offset + relative_offset,
                )),
                None => Ok(SegmentPosition::Done(self.next_absolute_offset())),
            },
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> u64 {
        match &self.inner {
            SegmentType::Memory(segment) => segment.len(),
        }
    }

    #[inline]
    pub(crate) fn size(&self) -> u64 {
        match &self.inner {
            SegmentType::Memory(segment) => segment.size(),
        }
    }
}
