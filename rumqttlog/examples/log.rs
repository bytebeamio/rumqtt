//! This example is used to test read and write throughput of the core of storage
//! NOTE write perf is only increasing till a certain buffer size. bigger sizes after that is causing a degrade
mod benchmarks;

use std::io;

use std::time::Instant;
use mqttlog::Log;

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

fn vread_records(log: &mut Log) -> Result<(), io::Error> {
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();

    let current_size = benchmarks::vread_records(log).unwrap();
    log.close_all().unwrap();

    benchmarks::report("vread.pb", current_size, start, guard);
    Ok(())
}
