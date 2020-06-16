use std::io;

#[derive(Debug)]
pub struct Index {
    /// Base offset
    base_offset: u64,
    /// Ids of the payloads
    ids: Vec<u64>,
    /// Positions of the payloads
    positions: Vec<u64>,
    /// Sizes of the payloads
    sizes: Vec<u64>,
}

impl Index {
    pub fn new(base_offset: u64) -> Index {
        Index {
            base_offset,
            ids: Vec::new(),
            positions: Vec::new(),
            sizes: Vec::new(),
        }
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Number of entries
    pub fn count(&self) -> u64 {
        self.sizes.len() as u64
    }

    pub fn write(&mut self, id: u64, pos: u64, len: u64) {
        self.ids.push(id);
        self.positions.push(pos);
        self.sizes.push(len);
    }

    /// Reads an offset from the index and returns segment record's offset, size and id
    pub fn read(&self, offset: u64) -> io::Result<(u64, u64, u64)> {
        if self.positions.len() == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "No entries in index"));
        }

        // get position of the segment. we could have directly calculated this
        // on segment directly but we need to keep the segment vanilla for vectored
        // writes and would have needed another vec for packet ids. Let's replacate
        // file storage index for consistency
        match self.positions.get(offset as usize) {
            Some(pos) => *pos,
            None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Reading at an invalid offset")),
        };

        let len = *self.sizes.get(offset as usize).unwrap();
        let id = *self.ids.get(offset as usize).unwrap();

        Ok((offset, len, id))
    }

    /// Returns starting position, size required to fit 'n' records, n (count)
    /// Total size of records might cross the provided boundary. Use returned size
    /// for allocating the buffer
    pub fn readv(&self, offset: u64, target_size: u64) -> io::Result<(u64, u64, Vec<u64>)> {
        let mut count = 0;
        let mut current_size = 0;
        let offset = offset as usize;

        if self.positions.len() == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "No entries in index"));
        }

        for size in self.sizes[offset..].iter() {
            current_size += size;
            count += 1;
            if current_size >= target_size {
                break;
            }
        }

        let ids = (&self.ids[offset..offset + count]).to_vec();
        Ok((offset as u64, current_size as u64, ids))
    }
}
