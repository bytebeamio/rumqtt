use std::time::Instant;
use mqttlog::volatile::Log;

mod benchmarks;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let payload_size = 1024;
    let count = 5_000_000;
    let total_size = payload_size as u64 * count;
    let mut payloads = benchmarks::payloads(payload_size, count).into_iter();
    let mut log = Log::new(500 * 1024, 10000).unwrap();
    for pkid in 0..count {
        let pkid = pkid % 65000;
        log.append(pkid as u16, payloads.next().unwrap()).unwrap();
    }

    let mut segment = 0;
    let mut offset = 0;
    let read_size = 100 * 1024;
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    for _i in 0..count {
        let (_, s, o, _, _, _data) = log.readv(segment, offset, read_size).unwrap();
        segment = s;
        offset = o + 1;
    }

    benchmarks::report("inmemoryread.pb", total_size as u64, start, guard);
}
