use super::Persistant;

/// A struct for keeping Bytes in memory.
#[derive(Debug, Clone)]
pub(crate) struct MemorySegment<T> {
    pub(crate) data: Vec<T>,
    total_size: u64,
}

impl<T> MemorySegment<T>
where
    T: Persistant + Clone,
{
    /// Create a new segment with given capacity.
    #[inline]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            total_size: 0,
        }
    }
    /// Create a new segment with given capacity, and the given `T` as first element.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn new(capacity: u64, inner_type: T) -> Self {
        let size = inner_type.size() as u64;
        let mut data = Vec::with_capacity(capacity as usize);
        data.push(inner_type);

        Self {
            data,
            total_size: size,
        }
    }

    /// Push a new `T` in the segment.
    #[inline]
    pub(crate) fn push(&mut self, inner_type: T) {
        self.total_size += inner_type.size() as u64;
        self.data.push(inner_type);
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

    /// Read a range of data into `out`. Returns the next read offset if possible else `None`.
    #[inline]
    pub(crate) fn readv(&self, index: u64, len: u64, out: &mut Vec<T>) -> Option<u64> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn memory_segment_works_for_bytes() {
        let mut mem_segment: MemorySegment<Bytes> = MemorySegment::with_capacity(10);
        let test_byte = Bytes::from_static(b"test1");
        mem_segment.push(test_byte.clone());
        assert_eq!(mem_segment.len(), 1);
        assert_eq!(mem_segment.last().unwrap(), test_byte);
    }

    #[test]
    fn readv_works_for_bytes() {
        let mut mem_segment: MemorySegment<Bytes> = MemorySegment::with_capacity(10);
        mem_segment.push(Bytes::from_static(b"test1"));
        mem_segment.push(Bytes::from_static(b"test2"));
        mem_segment.push(Bytes::from_static(b"test3"));
        mem_segment.push(Bytes::from_static(b"test4"));
        mem_segment.push(Bytes::from_static(b"test5"));
        mem_segment.push(Bytes::from_static(b"test6"));
        mem_segment.push(Bytes::from_static(b"test7"));
        mem_segment.push(Bytes::from_static(b"test8"));
        mem_segment.push(Bytes::from_static(b"test9"));
        assert_eq!(mem_segment.len(), 9);

        let mut out: Vec<Bytes> = Vec::new();
        let index = mem_segment.readv(0, 2, &mut out).unwrap();
        assert_eq!(index, 2);
        assert_eq!(
            out,
            vec![Bytes::from_static(b"test1"), Bytes::from_static(b"test2")]
        );
    }

    #[test]
    fn readv_works_for_vec_of_u8() {
        let mut mem_segment: MemorySegment<Vec<u8>> = MemorySegment::with_capacity(10);
        mem_segment.push(vec![1u8]);
        mem_segment.push(vec![2u8]);
        mem_segment.push(vec![3u8]);
        mem_segment.push(vec![4u8]);
        mem_segment.push(vec![5u8]);
        mem_segment.push(vec![6u8]);
        mem_segment.push(vec![7u8]);
        mem_segment.push(vec![8u8]);
        mem_segment.push(vec![9u8]);
        assert_eq!(mem_segment.len(), 9);

        let mut out: Vec<Vec<u8>> = Vec::new();
        let index = mem_segment.readv(0, 2, &mut out).unwrap();
        assert_eq!(index, 2);
        assert_eq!(out, vec![vec![1u8], vec![2u8]]);
    }
}
