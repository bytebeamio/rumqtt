use std::{fs, io};
use ::Log;

use crate::benchmarks::{generate_payload, BATCH_SIZE, DIR, MAX_SEGMENT_SIZE, RECORD_COUNT, RECORD_SIZE};

pub fn write_records(log: &mut Log) -> Result<u64, io::Error> {
    let record = generate_payload(RECORD_SIZE);
    for _i in 0..RECORD_COUNT {
        log.append(&record)?;
    }

    Ok(RECORD_SIZE * RECORD_COUNT)
}

pub fn vread_records(log: &mut Log) -> Result<u64, io::Error> {
    let mut next_base_offset = 0;
    let mut next_rel_offset = 0;
    let mut current_size = 0;
    loop {
        let (base_offset, rel_offset, _count, data) = log.readv(next_base_offset, next_rel_offset, BATCH_SIZE)?;
        next_base_offset = base_offset;
        next_rel_offset = rel_offset + 1;
        current_size += data.len() as u64;
        if current_size >= RECORD_SIZE * RECORD_COUNT {
            break;
        }
    }

    Ok(current_size)
}

pub fn log() -> Result<Log, io::Error> {
    let _ = fs::remove_dir_all(DIR);

    // We are setting maximum index size by considering all the segments. Which is a lot.
    // But this will demonstrate the affects of truncating the index file
    let max_index_size = RECORD_COUNT * 16;
    let log = Log::new(DIR, max_index_size, MAX_SEGMENT_SIZE, 100000000)?;
    Ok(log)
}
