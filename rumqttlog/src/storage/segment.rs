use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::path::PathBuf;

/// Segment of a storage. Writes go through a buffer writers to
/// reduce number of system calls. Reads are directly read from
/// the file as seek on buffer reader will dump the buffer anyway
/// Also multiple readers might be operating on a given segment
/// which makes the cursor movement very dynamic
pub struct Segment {
    file: File,
    writer: BufWriter<File>,
    size: u64,
    next_offset: u64,
    max_record_size: u64,
}

impl Segment {
    // TODO next offset should be initialized correctly for segments which are reconstructed. `append`
    // TODO without this will result in wrong offsets in the return position
    pub fn new<P: AsRef<Path>>(dir: P, base_offset: u64) -> io::Result<Segment> {
        let file_name = format!("{:020}.segment", base_offset);
        let file_path: PathBuf = dir.as_ref().join(file_name);
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)?;
        let metadata = file.metadata()?;

        // 1MB buffer size
        // NOTE write perf is only increasing till a certain buffer size. bigger sizes after that is causing a degrade
        let buf = BufWriter::with_capacity(1024 * 1024, file.try_clone()?);
        let size = metadata.len();

        let segment = Segment {
            file,
            writer: buf,
            size,
            next_offset: 0,
            max_record_size: 10 * 1024,
        };

        Ok(segment)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn set_next_offset(&mut self, next_offset: u64) {
        self.next_offset = next_offset;
    }

    /// Appends record to the file and return its offset
    pub fn append(&mut self, record: &[u8]) -> io::Result<(u64, u64)> {
        let record_size = record.len() as u64;
        if record_size > self.max_record_size {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Max record size exceeded",
            ));
        }

        // append record and increment size. cursor is moved to the end as per the docs
        // so we probably don't have to worry about reading and writing simultaneously
        self.writer.write_all(record)?;
        let position = self.size;
        self.size += record.len() as u64;

        // return current offset after incrementing next offset
        let offset = self.next_offset;
        self.next_offset += 1;

        Ok((offset, position))
    }

    /// Reads to fill the complete buffer. Returns number of bytes read
    pub fn read(&mut self, position: u64, buf: &mut [u8]) -> io::Result<u64> {
        // TODO: No need to flush segments which are already filled. Make this conditional and check perf
        self.writer.flush()?;
        self.read_at(position, buf)
    }

    #[inline]
    #[cfg(target_family = "unix")]
    fn read_at(&mut self, position: u64, mut buf: &mut [u8]) -> io::Result<u64> {
        use std::os::unix::fs::FileExt;

        self.file.read_exact_at(&mut buf, position)?;

        Ok(buf.len() as u64)
    }

    #[inline]
    #[cfg(target_family = "windows")]
    fn read_at(&mut self, position: u64, mut buf: &mut [u8]) -> io::Result<u64> {
        use std::io::{Read, Seek, SeekFrom};

        self.file.seek(SeekFrom::Start(position))?;
        self.file.read_exact(&mut buf)?;

        Ok(buf.len() as u64)
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::Segment;
    use pretty_assertions::assert_eq;

    #[test]
    fn second_time_initialization_happens_correctly() {
        let record = b"hello timestone commitlog";
        let len = record.len();
        let dir = tempfile::tempdir().unwrap();
        let base_offset = 10;

        // 1st boot
        {
            let mut segment = Segment::new(&dir, base_offset).unwrap();
            for i in 0..10 {
                let (offset, _pos) = segment.append(record).unwrap();
                assert_eq!(offset, i)
            }

            let mut next_pos = 0;
            for _i in 0..10 {
                let mut data = vec![0; len as usize];
                segment.read(next_pos, &mut data).unwrap();
                assert_eq!(&data, record);
                next_pos += len as u64;
            }

            segment.close().unwrap();
        }

        let record = b"iello timestone commitlog";

        // 2nd boot
        {
            let mut segment = Segment::new(&dir, base_offset).unwrap();
            // we usually get this from index
            segment.set_next_offset(10);
            let mut position = 0;
            for i in 10..20 {
                let (offset, pos) = segment.append(record).unwrap();
                position = pos;
                assert_eq!(offset, i)
            }

            let mut data = vec![0; len];
            segment.read(position, &mut data).unwrap();
            assert_eq!(&data, record);

            segment.close().unwrap();
        }
    }

    /*
    #[test]
    fn vectored_reads_works_as_expected() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = Segment::new(&dir, 10, 10 * 1024 * 1024).unwrap();

        // 100 1K appends
        for i in 0..100 {
            let record = vec![i; 1024];
            let (offset, _pos) = segment.append(&record).unwrap();
            assert_eq!(offset as u8, i)
        }

        for i in 0..100 {
            let data = segment.read(i).unwrap();
            assert_eq!(data[i as usize], i as u8);
        }

        // read 90K. leaves 10 elements
        let (count, _data) = segment.read_vectored(0, 90 * 1024).unwrap();
        assert_eq!(count, 90);

        // read remaining elements individually
        for i in 90..100 {
            let data = segment.read(i).unwrap();
            assert_eq!(data[i as usize], i as u8);
        }

        segment.close().unwrap();
    }
    */
}
