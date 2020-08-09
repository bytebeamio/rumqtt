use rumqttlog::Log;
use std::fs;
use std::io;

//use pretty_assertions::assert_eq;
// 10MB per segmet => 100000 records per segment
// 100 bytes hardcoded per record
// 1048576 records in a segment
const RECORD_COUNT: u64 = 100000;
const MAX_INDEX_SIZE: u64 = RECORD_COUNT * 16;
const MAX_SEGMENT_SIZE: u64 = 1 * 1000 * 1000;
const DIR: &str = "/tmp/timestone";

#[test]
fn smoke() -> Result<(), io::Error> {
    write_records(RECORD_COUNT)?;
    read_records(RECORD_COUNT)?;
    vread_records(RECORD_COUNT)?;
    Ok(())
}

// writes 100 byte payload 'count' times
fn write_records(count: u64) -> Result<(), io::Error> {
    let _ = fs::remove_dir_all(DIR);
    let _ = fs::create_dir_all("/tmp/timestone");
    let mut log = Log::new(DIR, MAX_INDEX_SIZE, MAX_SEGMENT_SIZE, 100000000)?;

    for i in 0..count {
        // 100 bytes payload
        let data = format!("{:0100}", i);
        let data: Vec<u8> = data.into();
        log.append(&data)?;
    }

    log.close_all().unwrap();
    Ok(())
}

fn read_records(count: u64) -> Result<(), io::Error> {
    let mut log = Log::new(DIR, MAX_INDEX_SIZE, MAX_SEGMENT_SIZE, 100000000)?;
    let mut total_count = 0;
    let mut base_offset = 0;
    let mut rel_offset = 0;

    loop {
        let data = match log.read(base_offset, rel_offset) {
            Ok(data) => data,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                base_offset += rel_offset;
                rel_offset = 0;
                continue;
            }
            Err(e) => return Err(e),
        };

        let data = String::from_utf8(data).unwrap();
        let expected = format!("{:0100}", total_count);

        assert_eq!(expected, data);
        rel_offset += 1;
        total_count += 1;
        if total_count >= count {
            break;
        }
    }

    log.close_all().unwrap();
    Ok(())
}

/// Tries to read 450 bytes in bulk. Which should traslate to 5 100 byte records by the index
// we read 5 segments at a time in vectored reads. 100 bytes each
// multiples of 5 a time cross segment boundaries (which is a good test)
fn vread_records(count: u64) -> Result<(), io::Error> {
    let mut log = Log::new(DIR, MAX_INDEX_SIZE, MAX_SEGMENT_SIZE, 100000000)?;
    let mut next_base_offset = 0;
    let mut next_rel_offset = 0;
    let mut current_count = 0;
    loop {
        let (base_offset, rel_offset, cnt, data) =
            log.readv(next_base_offset, next_rel_offset, 500)?;
        let records = String::from_utf8(data).unwrap();
        // number of records in a batch. boundaries of a segment might've less than 5 records
        let record_count = records.len() / 100;
        for i in 0..record_count {
            let expected = format!("{:0100}", current_count + i as u64);
            let start = i * 100;
            let end = start + 100;
            let record = &records[start..end];
            assert_eq!(record, expected);
        }

        current_count += cnt;
        next_base_offset = base_offset;
        next_rel_offset = rel_offset + 1;
        if current_count >= count {
            break;
        }
    }

    Ok(())
}
