/*
//! This example is used to test read and write throughput of the core of storage
//! NOTE write perf is only increasing till a certain buffer size. bigger sizes after that is causing a degrade

pub mod benchmarks;

use std::fs::{self, OpenOptions};
use std::io;
use std::time::Instant;
use mqttlog::Segment;

const SEGMENT: &str = "/tmp/timestone/00000000000000000010.segment";

fn main() -> Result<(), io::Error> {
    let _ = fs::remove_dir_all("/tmp/timestone");
    let _ = fs::create_dir_all("/tmp/timestone");

    let record_count = 1 * 1000 * 1024;
    let record_size = 1024;
    let mut segment = Segment::new("/tmp/timestone", 10)?;

    write_records(&mut segment, record_size, record_count)?;
    read_records(&mut segment, record_size as u64, record_count)?;
    vread_records(&mut segment, 10 * 1024 * 1024, record_count * record_size as u64)?;
    Ok(())
}

fn write_records(segment: &mut Segment, payload_size: u64, count: u64) -> Result<(), io::Error> {
    let record = benchmarks::generate_payload(payload_size);
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    for _i in 0..count {
        segment.append(&record)?;
    }

    segment.close().unwrap();

    let file = OpenOptions::new().read(true).open(SEGMENT)?;
    let current_size = file.metadata()?.len();
    benchmarks::report("vread.pb", current_size, start, guard);
    Ok(())
}

fn read_records(segment: &mut Segment, record_size: u64, count: u64) -> Result<(), io::Error> {
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    let mut position = 0;
    for _i in 0..count {
        let mut data = vec![0; record_size as usize];
        segment.read(position, &mut data)?;
        position += record_size;
    }

    let file = OpenOptions::new().read(true).open(SEGMENT)?;
    let current_size = file.metadata()?.len();
    benchmarks::report("vread.pb", current_size, start, guard);
    Ok(())
}

fn vread_records(segment: &mut Segment, batch_size: u64, total_size: u64) -> Result<(), io::Error> {
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();

    let mut current_size = 0;
    let mut position = 0;
    loop {
        let mut data = vec![0; batch_size as usize];
        segment.read(position, &mut data)?;
        current_size += data.len() as u64;
        position += batch_size;

        if current_size >= total_size {
            break;
        }
    }

    benchmarks::report("vread.pb", current_size, start, guard);
    Ok(())
}
 */

fn main() {}
