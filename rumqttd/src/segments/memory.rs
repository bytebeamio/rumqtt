use bytes::Bytes;

/// A struct for keeping Bytes in memory.
#[derive(Debug, Clone)]
pub(crate) struct MemorySegment {
    pub(crate) data: Vec<Bytes>,
    size: u64,
}

impl MemorySegment {
    /// Create a new segment with given capacity.
    #[inline]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            size: 0,
        }
    }

    /// Create a new segment with given capacity, and the given `Bytes` first element.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn new(capacity: u64, byte: Bytes) -> Self {
        let size = byte.len() as u64;
        let mut data = Vec::with_capacity(capacity as usize);
        data.push(byte);

        Self { data, size }
    }

    /// Push a new `Bytes` in the segment.
    #[inline]
    pub(crate) fn push(&mut self, byte: Bytes) {
        self.size += byte.len() as u64;
        self.data.push(byte);
    }

    /// Get the number of `Bytes` in the segment.
    #[inline]
    pub(crate) fn len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Get the total size in bytes of the segment.
    #[inline]
    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    #[inline]
    pub fn last(&self) -> Option<Bytes> {
        self.data.last().cloned()
    }

    /// Read a range of data into `out`. Returns the next read offset if possible else `None`.
    #[inline]
    pub(crate) fn readv(&self, index: u64, len: u64, out: &mut Vec<Bytes>) -> Option<u64> {
        if index >= self.len() {
            return None;
        }

        let mut limit = index + len;
        let mut ret = Some(limit);
        if limit >= self.len() {
            ret = None;
            limit = self.len();
        }
        out.extend(self.data[index as usize..limit as usize].iter().cloned());
        ret
    }
}
