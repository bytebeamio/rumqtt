use rumqttlog::volatile::Log;
use std::time::Instant;

mod common;

fn main() {
    let payload_size = 1024;
    let count = 5_000_000;
    let mut payloads = common::payloads(payload_size, count).into_iter();
    let total_size = payload_size as u64 * count;

    let mut log = Log::new(500 * 1024, 10000);
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    for _i in 0..count {
        log.append(payloads.next().unwrap());
    }

    common::report("inmemorywrite.pb", total_size as u64, start, guard);
}
