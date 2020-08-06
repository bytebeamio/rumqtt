use bytes::Bytes;

/// Segment of a storage. Writes go through a buffer writers to
/// reduce number of system calls. Reads are directly read from
/// the file as seek on buffer reader will dump the buffer anyway
/// Also multiple readers might be operating on a given segment
/// which makes the cursor movement very dynamic
pub struct Segment {
    base_offset: u64,
    size: usize,
    next_offset: u64,
    pub(crate) file: Vec<Bytes>,
}

impl Segment {
    pub fn new(base_offset: u64) -> Segment {
        let file = Vec::with_capacity(10000);

        Segment {
            base_offset,
            file,
            size: 0,
            next_offset: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Appends record to the file and return its offset
    pub fn append(&mut self, record: Bytes) -> u64 {
        let len = record.len();
        self.file.push(record);


        self.size += len;

        // return current offset after incrementing next offset
        let offset = self.next_offset;
        self.next_offset += 1;

        offset
    }

    /// Reads to fill the complete buffer. Returns number of bytes reamodd
    pub fn read(&mut self, offset: usize) -> Option<Bytes> {
        match self.file.get(offset) {
            Some(record) => Some(record.clone()),
            None => None
        }
    }

    /// Reads multiple data from a segment
    pub fn readv(&self, offset: usize) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(100);

        for d in self.file[offset..].iter() {
            out.push(d.clone());
        }

        out
    }
}
