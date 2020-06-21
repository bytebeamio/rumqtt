pub mod index;
pub mod segment;

use index::Index;
use segment::Segment;

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;

struct Chunk {
    index: Index,
    segment: Segment,
}

pub struct Log {
    dir: PathBuf,
    max_segment_size: u64,
    max_index_size: u64,
    base_offsets: Vec<u64>,
    max_segments: usize,
    active_chunk: u64,
    chunks: HashMap<u64, Chunk>,
}

impl Log {
    pub fn new<P: Into<PathBuf>>(
        dir: P,
        max_index_size: u64,
        max_segment_size: u64,
        max_segments: usize,
    ) -> io::Result<Log> {
        let dir = dir.into();
        let _ = fs::create_dir_all(&dir);
        if max_segment_size < 1024 || max_index_size < 100 {
            panic!("size should be at least 1KB")
        }

        let files = fs::read_dir(&dir)?;
        let mut base_offsets = Vec::new();
        for file in files {
            let path = file?.path();
            let offset = path.file_stem().unwrap().to_str().unwrap();
            let offset = offset.parse::<u64>().unwrap();
            base_offsets.push(offset);
        }

        base_offsets.sort();
        let mut chunks = HashMap::new();

        let active_segment = if let Some((last_offset, offsets)) = base_offsets.split_last() {
            // Initialized filled segments
            for base_offset in offsets.iter() {
                let index = Index::new(&dir, *base_offset, max_index_size, false)?;
                let segment = Segment::new(&dir, *base_offset)?;
                let chunk = Chunk { index, segment };
                chunks.insert(*base_offset, chunk);
            }

            // Initialize active segment
            let index = Index::new(&dir, *last_offset, max_index_size, true)?;
            let segment = Segment::new(&dir, *last_offset)?;
            let mut chunk = Chunk { index, segment };

            // Wrong counts due to unclosed segments are handled during initialization. We can just assume
            // count is always right from here on
            let next_offset = chunk.index.count();
            chunk.segment.set_next_offset(next_offset);
            chunks.insert(*last_offset, chunk);
            *last_offset
        } else {
            let index = Index::new(&dir, 0, max_index_size, true)?;
            let segment = Segment::new(&dir, 0)?;
            let chunk = Chunk { index, segment };
            chunks.insert(0, chunk);
            base_offsets.push(0);
            0
        };

        let log = Log {
            dir,
            max_segment_size,
            max_index_size,
            max_segments,
            base_offsets,
            chunks,
            active_chunk: active_segment,
        };

        Ok(log)
    }

    pub fn append(&mut self, record: &[u8]) -> io::Result<()> {
        let active_chunk = if let Some(v) = self.chunks.get_mut(&self.active_chunk) {
            v
        } else {
            return Err(io::Error::new(io::ErrorKind::Other, "No active segment"));
        };

        if active_chunk.segment.size() >= self.max_segment_size {
            active_chunk.segment.close()?;
            active_chunk.index.close()?;

            // update active chunk
            let base_offset = active_chunk.index.base_offset() + active_chunk.index.count();
            let index = Index::new(&self.dir, base_offset, self.max_index_size, true)?;
            let segment = Segment::new(&self.dir, base_offset)?;
            let chunk = Chunk { index, segment };
            self.chunks.insert(base_offset, chunk);
            self.base_offsets.push(base_offset);
            self.active_chunk = base_offset;

            if self.base_offsets.len() > self.max_segments {
                let remove_offset = self.base_offsets.remove(0);
                self.remove(remove_offset)?;
            }
        }

        // write record to segment and index
        let active_chunk = self.chunks.get_mut(&self.active_chunk).unwrap();
        let (_, position) = active_chunk.segment.append(record)?;
        active_chunk.index.write(position, record.len() as u64)?;
        Ok(())
    }

    /// Read a record from correct segment
    /// Returns data, next base offset and relative offset
    pub fn read(&mut self, base_offset: u64, offset: u64) -> io::Result<Vec<u8>> {
        let chunk = match self.chunks.get_mut(&base_offset) {
            Some(segment) => segment,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid segment",
                ))
            }
        };

        let (position, len) = chunk.index.read(offset)?;
        let mut payload = vec![0; len as usize];
        chunk.segment.read(position, &mut payload)?;
        Ok(payload)
    }

    /// Goes through index and returns chunks which tell how to sweep segments to collect
    /// necessary amount on data asked by the user
    /// Corner cases:
    /// When there is more data (in other segments) current eof should move to next segment
    /// Empty segments are possible after moving to next segment
    /// EOFs after some data is collected are not errors
    fn indexv(&self, base_offset: u64, relative_offset: u64, size: u64) -> io::Result<Chunks> {
        let mut chunks = Chunks {
            base_offset,
            relative_offset,
            count: 0,
            size: 0,
            chunks: Vec::new(),
        };

        loop {
            // Get the chunk with given base offset
            let chunk = match self.chunks.get(&chunks.base_offset) {
                Some(c) => c,
                None if chunks.count == 0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Invalid segment",
                    ))
                }
                None => break,
            };

            // If next relative offset is equal to index count => We've crossed the boundary
            // NOTE: We are assuming the index file was closed properly. `index.count()` will
            // count `unfilled zeros` due to mmap `set_len` if it was not closed properly
            // FIXME for chunks with index which isn't closed properly, relative offset will
            // FIXME be less than count but `readv` is going to return EOF
            // Reads on indexes which aren't closed properly result in `EOF` when they encounter 0 length record as the mmaped
            // segment isn't truncated. Index read goes past the actual size as the size calculation of the next boot is wrong.
            // This block covers both usual EOFs during normal operations as well as EOFs due to unclosed index
            // EOF due to unclosed index is a warning though
            if chunks.relative_offset >= chunk.index.count() {
                // break if we are already at the tail segment
                if chunks.base_offset == *self.base_offsets.last().unwrap() {
                    chunks.relative_offset -= 1;
                    break;
                }

                // we use 'total offsets' to go next segment. this remains same during subsequent
                // tail reads if there are no appends. hence the above early return
                chunks.base_offset = chunk.index.base_offset() + chunk.index.count();
                chunks.relative_offset = 0;
                continue;
            }

            // Get what to read from the segment and fill the buffer. Covers the case where the logic has just moved to next
            // segment and the segment is empty
            let read_size = size - chunks.size;
            let (position, payload_size, count) =
                chunk.index.readv(chunks.relative_offset, read_size)?;
            chunks.relative_offset += count;
            chunks.count += count;
            chunks.size += payload_size;
            chunks
                .chunks
                .push((chunks.base_offset, position, payload_size, count));
            if chunks.size >= size {
                chunks.relative_offset -= 1;
                break;
            }
        }

        Ok(chunks)
    }

    /// Reads multiple packets from the storage and return base offset and relative offset of the
    /// Returns base offset, relative offset of the last record along with number of messages and count
    /// Goes to next segment when relative off set crosses boundary
    pub fn readv(
        &mut self,
        base_offset: u64,
        relative_offset: u64,
        size: u64,
    ) -> io::Result<(u64, u64, u64, Vec<u8>)> {
        let chunks = self.indexv(base_offset, relative_offset, size)?;

        // Fill the pre-allocated buffer
        let mut out = vec![0; chunks.size as usize];
        let mut start = 0;
        for c in chunks.chunks {
            let chunk = match self.chunks.get_mut(&c.0) {
                Some(c) => c,
                None => break,
            };

            let position = c.1;
            let payload_size = c.2;
            chunk
                .segment
                .read(position, &mut out[start..start + payload_size as usize])?;
            start += payload_size as usize;
        }

        Ok((
            chunks.base_offset,
            chunks.relative_offset,
            chunks.count,
            out,
        ))
    }

    pub fn close(&mut self, base_offset: u64) -> io::Result<()> {
        if let Some(chunk) = self.chunks.get_mut(&base_offset) {
            chunk.index.close()?;
            chunk.segment.close()?;
        }

        Ok(())
    }

    // Removes segment with given base offset from the storage and the system
    pub fn remove(&mut self, base_offset: u64) -> io::Result<()> {
        if let Some(mut chunk) = self.chunks.remove(&base_offset) {
            chunk.segment.close()?;

            let file: PathBuf = self.dir.clone();
            let index_file_name = format!("{:020}.index", base_offset);
            let segment_file_name = format!("{:020}.segment", base_offset);

            // dbg!(file.join(&index_file_name));
            fs::remove_file(file.join(index_file_name))?;
            fs::remove_file(file.join(segment_file_name))?;
        }

        Ok(())
    }

    pub fn close_all(&mut self) -> io::Result<()> {
        for (_, chunk) in self.chunks.iter_mut() {
            chunk.index.close()?;
            chunk.segment.close()?;
        }

        Ok(())
    }

    pub fn remove_all(&mut self) -> io::Result<()> {
        self.close_all()?;
        fs::remove_dir(&self.dir)?;

        Ok(())
    }
}

/// Captured state while sweeping indexes collect a bulk of records
/// from segment/segments
/// TODO: 'chunks' vector arguments aren't readable
struct Chunks {
    base_offset: u64,
    relative_offset: u64,
    count: u64,
    size: u64,
    chunks: Vec<(u64, u64, u64, u64)>,
}

#[cfg(test)]
mod test {
    use super::Log;
    use pretty_assertions::assert_eq;
    use std::io;

    #[test]
    fn append_creates_and_deletes_segments_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let record_count = 100;
        let max_index_size = record_count * 16;
        let mut log = Log::new(dir, max_index_size, 10 * 1024, 10).unwrap();
        let mut payload = vec![0u8; 1024];

        // 200 1K iterations. 20 files ignoring deletes. 0.segment, 10.segment .... 199.segment
        // considering deletes -> 110.segment .. 200.segment
        for i in 0..200 {
            payload[0] = i;
            log.append(&payload).unwrap();
        }

        // Semi fill 200.segment
        for i in 200..205 {
            payload[0] = i;
            log.append(&payload).unwrap();
        }

        let data = log.read(10, 0);
        match data {
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => (),
            _ => panic!("Expecting an invalid input error"),
        };

        // read segment with base offset 110
        let base_offset = 110;
        for i in 0..10 {
            let data = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
        }

        // read segment with base offset 190
        let base_offset = 110;
        for i in 0..10 {
            let data = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
        }

        // read 200.segment which is semi filled with 5 records
        let base_offset = 200;
        for i in 0..5 {
            let data = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
        }

        let data = log.read(base_offset, 5);
        match data {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => (),
            _ => panic!("Expecting end of file error"),
        };
    }

    #[test]
    fn multi_segment_reads_work_as_expected() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        // 100K bytes
        let record_count = 100;
        let record_size = 1 * 1024;

        // 10 records per segment. 10 segments (0.segment - 90.segment)
        let max_segment_size = 10 * 1024;
        let max_index_size = record_count * 16;
        let mut log = Log::new(dir, max_index_size, max_segment_size, 100).unwrap();

        // 100 1K iterations. 10 files ignoring deletes.
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 90.segment (0 size)
        // 10K per file
        let mut payload = vec![0u8; record_size];
        for i in 0..record_count {
            payload[0] = i as u8;
            log.append(&payload).unwrap();
        }

        // Read all the segments
        let base_offset = 0;
        for i in 0..10 {
            let data = log.read(base_offset, i).unwrap();
            let d = (base_offset + i) as u8;
            assert_eq!(data[0], d);
        }
    }

    #[test]
    fn vectored_read_works_as_expected() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let record_count = 100;
        let max_index_size = record_count * 16;
        let mut log = Log::new(dir, max_index_size, 10 * 1024, 10).unwrap();

        // 90 1K iterations. 10 files ignoring deletes.
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 90.segment (0 size)
        // 10K per file
        let mut payload = vec![0u8; 1024];
        for i in 0..90 {
            payload[0] = i;
            log.append(&payload).unwrap();
        }

        // Read 50K. Reads 0.segment - 4.segment
        let (base_offset, relative_offset, count, data) = log.readv(0, 0, 50 * 1024).unwrap();
        assert_eq!(base_offset, 40);
        assert_eq!(relative_offset, 9);
        assert_eq!(count, 50);

        let total_size = data.len();
        assert_eq!(total_size, 50 * 1024);

        // Read 50.segment offset 0
        let data = log.read(50, 0).unwrap();
        assert_eq!(data[0], 50);
    }

    #[test]
    fn vectored_reads_in_different_boots_works_as_expected() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let record_count = 100;
        let max_index_size = record_count * 16;
        let mut log = Log::new(dir, max_index_size, 10 * 1024, 10).unwrap();

        // 100 1K iterations. 10 files
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 90.segment (90 - 99)
        // 10K per file
        let mut payload: Vec<u8> = vec![0u8; 1024];
        for i in 0..100 {
            payload[0] = i;
            log.append(&payload).unwrap();
        }

        log.close_all().unwrap();

        // Boot 2. Read 50K. Reads 0.segment - 4.segment
        let mut log = Log::new(dir, max_index_size, 10 * 1024, 10).unwrap();
        let (base_offset, relative_offset, count, data) = log.readv(0, 0, 50 * 1024).unwrap();
        assert_eq!(base_offset, 40);
        assert_eq!(relative_offset, 9);
        assert_eq!(count, 50);

        for i in 0..count {
            let start = i as usize * 1024;
            let end = start + 1024;
            let record = &data[start..end];
            assert_eq!(record[0], i as u8);
        }

        let total_size = data.len();
        assert_eq!(total_size, 50 * 1024);

        // Read 50.segment offset 0
        let data = log.read(50, 0).unwrap();
        assert_eq!(data[0], 50);
    }

    #[test]
    fn vectored_reads_on_unclosed_index_and_segment_works_as_expected() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        // 15K bytes
        let record_count = 15;
        let record_size = 1 * 1024;

        let max_segment_size = 10 * 1024;
        let max_index_size = record_count * 16;
        let mut log = Log::new(dir, max_index_size, max_segment_size, 100).unwrap();

        // 10 records per segment. 2 segments. 0.segment, 10.segment (partially filled and unclosed)
        let mut payload = vec![0u8; record_size];
        for i in 0..record_count {
            payload[0] = i as u8;
            log.append(&payload).unwrap();
        }

        // Last storage not closed. Index will be filled with zeros and segment entries in index are not flushed from buffer yet
        // Trailing zero indexes are considered as corrupted indexes
        if let Ok(_l) = Log::new(dir, max_index_size, max_segment_size, 100) {
            panic!("Expecting a corrupted index error due to trailing zeros in the index")
        }
    }

    #[test]
    fn vectored_reads_crosses_boundary_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let record_count = 100;
        let max_index_size = record_count * 16;
        let mut log = Log::new(dir, max_index_size, 10 * 1024, 10).unwrap();

        // 25 1K iterations. 3 segments
        // 0.segment (10K, data with 0 - 9), 10.segment (5K, data with 10 - 14)
        let mut payload = vec![0u8; 1024];
        for i in 0..25 {
            payload[0] = i;
            log.append(&payload).unwrap();
        }

        // Read 15K. Crosses boundaries of the segment and offset will be in the middle of 2nd segment
        let (base_offset, relative_offset, count, data) = log.readv(0, 0, 15 * 1024).unwrap();
        assert_eq!(base_offset, 10);
        assert_eq!(relative_offset, 4);
        assert_eq!(count, 15);
        assert_eq!(data.len(), 15 * 1024);

        // Read 15K. Crosses boundaries of the segment and offset will be at last record of 3rd segment
        let (base_offset, relative_offset, count, data) = log
            .readv(base_offset, relative_offset + 1, 15 * 1024)
            .unwrap();
        assert_eq!(base_offset, 20);
        assert_eq!(relative_offset, 4);
        assert_eq!(count, 10);
        assert_eq!(data.len(), 10 * 1024);
    }

    #[test]
    fn vectored_read_more_than_full_chomp_works_as_expected() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();

        let record_count = 100;
        let max_index_size = record_count * 16;
        let mut log = Log::new(dir, max_index_size, 10 * 1024, 10).unwrap();

        // 90 1K iterations. 10 files
        // 0.segment (data with 0 - 9), 10.segment (10 - 19) .... 80.segment
        // 10K per file. 90K in total
        let mut payload = vec![0u8; 1024];
        for i in 0..90 {
            payload[0] = i;
            log.append(&payload).unwrap();
        }

        // Read 200K. Crosses boundaries of all the segments
        let (base_offset, relative_offset, count, data) = log.readv(0, 0, 200 * 1024).unwrap();
        assert_eq!(base_offset, 80);
        assert_eq!(relative_offset, 9);
        assert_eq!(count, 90);
        assert_eq!(data.len(), 90 * 1024);
    }
}
