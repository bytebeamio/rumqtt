use std::time::Instant;
mod common;

use rumqttlog::volatile::Log;

fn main() {
    let payload_size = 1024;
    let count = 5_000_000;
    let total_size = payload_size as u64 * count;
    let mut payloads = common::payloads(payload_size, count).into_iter();
    let mut log = Log::new(500 * 1024, 10000);
    for _ in 0..count {
        log.append(payloads.next().unwrap());
    }

    let mut segment = 0;
    let mut offset = 0;
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    for _i in 0..count {
        let (_, s, o, _, _data) = log.readv(segment, offset, 0);
        segment = s;
        offset = o + 1;
    }

    common::report("inmemoryread.pb", total_size as u64, start, guard);
}
