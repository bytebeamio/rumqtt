use bytes::Bytes;
use std::io;

/// Segment of a storage. Writes go through a buffer writers to
/// reduce number of system calls. Reads are directly read from
/// the file as seek on buffer reader will dump the buffer anyway
/// Also multiple readers might be operating on a given segment
/// which makes the cursor movement very dynamic
pub struct Segment {
    _base_offset: u64,
    file: Vec<Bytes>,
    size: u64,
    next_offset: u64,
    max_record_size: u64,
}

impl Segment {
    pub fn new(base_offset: u64) -> io::Result<Segment> {
        let file = Vec::with_capacity(10000);

        let segment = Segment {
            _base_offset: base_offset,
            file,
            size: 0,
            next_offset: 0,
            max_record_size: 10 * 1024,
        };

        Ok(segment)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Appends record to the file and return its offset
    pub fn append(&mut self, record: Bytes) -> io::Result<(u64, u64)> {
        let record_size = record.len() as u64;
        if record_size > self.max_record_size {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Max record size exceeded",
            ));
        }

        // append record and increment size. cursor is moved to the end as per the docs
        // so we probably don't have to worry about reading and writing simultaneously
        let len = record.len();
        self.file.push(record);
        let position = self.size;
        self.size += len as u64;

        // return current offset after incrementing next offset
        let offset = self.next_offset;
        self.next_offset += 1;

        Ok((offset, position))
    }

    /// Reads to fill the complete buffer. Returns number of bytes read
    pub fn read(&mut self, offset: u64) -> Bytes {
        self.file.get(offset as usize).unwrap().clone()
    }

    /// Reads to fill the complete buffer. Returns number of bytes read
    pub fn readv(&mut self, offset: u64, count: u64) -> Vec<Bytes> {
        let mut out = Vec::with_capacity(100);
        // TODO Return total size
        // let mut current_size = 0;

        let start = offset as usize;
        let end = start + count as usize;
        for d in self.file[start..end].iter() {
            // dbg!(d);
            out.push(d.clone());
            // current_size += d.len();
        }

        out
    }
}
