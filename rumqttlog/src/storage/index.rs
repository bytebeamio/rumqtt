use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use memmap::MmapMut;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const POS_WIDTH: u64 = 8;
const LEN_WIDTH: u64 = 8;
const ENTRY_WIDTH: u64 = POS_WIDTH + LEN_WIDTH;

pub struct Index {
    base_offset: u64,
    file: File,
    mmap: MmapMut,
    pub(crate) size: u64,
    pub(crate) max_size: u64,
}

impl Index {
    pub fn new<P: AsRef<Path>>(
        dir: P,
        base_offset: u64,
        max_size: u64,
        active: bool,
    ) -> io::Result<Index> {
        let file_name = format!("{:020}.index", base_offset);
        let file_path: PathBuf = dir.as_ref().join(file_name);
        let verify = file_path.exists();

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)?;
        let metadata = file.metadata()?;
        let size = metadata.len() as u64;

        // truncate and mmap the file
        // Old segment indexes are properly closed which shrinks the size of index file form maximum size
        // Old segment indexes are immutable and hence we freeze the size to file size.
        // For active segments, we set the size to max to be able to append more segment information
        if active {
            file.set_len(max_size)?;
        } else {
            file.set_len(size)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let index = Index {
            base_offset,
            file,
            mmap,
            size,
            max_size,
        };

        if verify {
            index.verify()?;
        }

        Ok(index)
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Number of entries
    pub fn count(&self) -> u64 {
        self.size / ENTRY_WIDTH
    }

    /// Index files which aren't closed will contains zeros as the mmap file wouldn't be truncated
    /// Treating these files as corrupted will free a lot of special case code in index and segment
    /// Facilitates easier intuition of logic & segment appends won't return wrong offset due to
    /// incorrect size. We can just do size based segment jumps and use ? for error handling
    fn verify(&self) -> io::Result<()> {
        let count = self.count();
        if count == 0 {
            let e = format!("Index {} should contain some data", self.base_offset);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }

        let (position, len) = self.read(count - 1)?;
        if position == 0 || len == 0 {
            let e = format!(
                "Index {} has trailing 0s. Index corrupted",
                self.base_offset
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }

        Ok(())
    }

    pub fn write(&mut self, pos: u64, len: u64) -> io::Result<()> {
        if self.size + ENTRY_WIDTH > self.max_size {
            return Err(io::Error::new(io::ErrorKind::Other, "Index full"));
        }

        let start = self.size as usize;
        let end = start + POS_WIDTH as usize;
        let mut buf = &mut self.mmap.as_mut()[start..end];
        buf.write_u64::<BigEndian>(pos)?;

        let start = end;
        let end = start + LEN_WIDTH as usize;
        let mut buf = &mut self.mmap.as_mut()[start..end];
        buf.write_u64::<BigEndian>(len)?;

        self.size += ENTRY_WIDTH;
        Ok(())
    }

    /// Reads an offset from the index and returns segment record's position and size
    pub fn read(&self, offset: u64) -> io::Result<(u64, u64)> {
        if self.size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "No entries in index",
            ));
        }

        // entry of the target offset
        let entry_position = offset * ENTRY_WIDTH;

        // reading at invalid postion from a file is implementation dependent. handle this explicitly
        // https://doc.rust-lang.org/std/io/trait.Seek.html#tymethod.seek
        if self.size < entry_position + ENTRY_WIDTH {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Reading at an invalid offset",
            ));
        }

        // read position
        let start = entry_position as usize;
        let end = start + POS_WIDTH as usize;
        let mut buf = &self.mmap[start..end];
        let position = buf.read_u64::<BigEndian>()?;

        // read len
        let start = end;
        let end = start + LEN_WIDTH as usize;
        let mut buf = &self.mmap[start..end];
        let len = buf.read_u64::<BigEndian>()?;

        Ok((position, len))
    }

    /// Returns starting position, size required to fit 'n' records, n (count)
    /// Total size of records might cross the provided boundary. Use returned size
    /// for allocating the buffer
    pub fn readv(&self, offset: u64, size: u64) -> io::Result<(u64, u64, u64)> {
        let mut count = 0;
        let mut current_size = 0;
        let mut next_offset = offset;

        // read the first record and fail if there is a problem
        let (pos, len) = self.read(offset)?;
        let mut last_record_size = len;
        let start_position = pos;

        // all the errors here are soft failures which are just used to break the loop
        loop {
            // size reached. include the last record even though it crosses boundary
            current_size += last_record_size;
            next_offset += 1;
            count += 1;
            if current_size >= size {
                break;
            }

            // estimate size till the previous record by reading current offset
            let (_, len) = match self.read(next_offset) {
                Ok(v) => v,
                Err(_e) => break,
            };

            last_record_size = len;
        }

        Ok((start_position, current_size, count))
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.mmap.flush()?;
        self.file.flush()?;
        self.file.set_len(self.size)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::Index;
    use tempfile::tempdir;

    fn write_entries(index: &mut Index, entries: Vec<(u64, u64)>) {
        for (offset, (position, len)) in entries.into_iter().enumerate() {
            let offset = offset as u64;
            index.write(position, len).unwrap();
            let (p, l) = index.read(offset).unwrap();
            assert_eq!(len, l);
            assert_eq!(position, p);
        }
    }

    #[test]
    fn write_and_read_works_as_expected() {
        let path = tempdir().unwrap();

        // create a new index
        let mut index = Index::new(&path, 0, 1024, true).unwrap();
        assert!(index.read(0).is_err());

        let entries = vec![(0, 100), (100, 100), (200, 600), (800, 200), (1000, 100)];

        // write and read index
        write_entries(&mut index, entries.clone());
        assert!(index.read(5).is_err());
        index.close().unwrap();

        // try reading from an offset which is out of boundary
        let out_of_boundary_offset = entries.len() as u64;
        let o = index.read(out_of_boundary_offset);
        assert!(o.is_err());

        // reconstruct the index from file
        let index = Index::new(path, 0, 1024, true).unwrap();
        let offset = index.count() - 1;
        assert_eq!(offset, 4);
    }

    #[test]
    fn bulk_reads_work_as_expected() {
        let path = tempdir().unwrap();

        // create a new index
        let mut index = Index::new(&path, 0, 1024, true).unwrap();
        assert!(index.read(0).is_err());

        // vectors (position, size of the record)
        let entries = vec![(0, 100), (100, 100), (200, 600), (800, 200), (1000, 100)];
        write_entries(&mut index, entries);
        index.close().unwrap();

        let (position, size, count) = index.readv(1, 1024).unwrap();
        assert_eq!(position, 100);
        assert_eq!(size, 1000);
        assert_eq!(count, 4);

        let entries = vec![(0, 100), (100, 100), (200, 600), (800, 200), (1000, 100)];
        write_entries(&mut index, entries);
        index.close().unwrap();

        // read less than size of a single record
        let (offset, size, count) = index.readv(2, 100).unwrap();
        assert_eq!(offset, 200);
        assert_eq!(size, 600);
        assert_eq!(count, 1);
    }
}
