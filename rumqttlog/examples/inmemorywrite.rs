use std::time::Instant;
use mqttlog::volatile::Log;

mod benchmarks;

fn main() {
    let payload_size = 1024;
    let count = 5_000_000;
    let mut payloads = benchmarks::payloads(payload_size, count).into_iter();
    let total_size = payload_size as u64 * count;

    let pkid = 0;
    let mut log = Log::new(500 * 1024, 10000).unwrap();
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    for _i in 0..count {
        log.append(pkid, payloads.next().unwrap()).unwrap();
    }

    benchmarks::report("inmemorywrite.pb", total_size as u64, start, guard);
}
