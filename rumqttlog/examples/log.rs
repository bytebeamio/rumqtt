/*
//! This example is used to test read and write throughput of the core of storage
//! NOTE write perf is only increasing till a certain buffer size. bigger sizes after that is causing a degrade
mod common;

use std::io;

use std::time::Instant;
use rumqttlog::Log;

fn main() -> Result<(), io::Error> {
    let mut log = benchmarks::log().unwrap();
    write_records(&mut log)?;
    vread_records(&mut log)?;
    Ok(())
}

fn write_records(log: &mut Log) -> Result<(), io::Error> {
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    let current_size = benchmarks::write_records(log).unwrap();
    log.close_all().unwrap();
    benchmarks::report("append.pb", current_size, start, guard);
    Ok(())
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
*/

fn main() {}
