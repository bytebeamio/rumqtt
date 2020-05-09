pub mod benchmarks;

use std::time::Instant;
use mqttlog::{CommitLog, Config};

// 1M records. 1K per record. 1GB in total
pub const RECORD_COUNT: u64 = 1 * 1024 * 1024;
pub const RECORD_SIZE: u64 = 1 * 1024;

// 100MB per segment. 100K records per segment. 10 segments
const MAX_SEGMENT_SIZE: u64 = 100 * 1024 * 1024;
const MAX_SEGMENT_RECORDS: u64 = 1_000_000;
const MAX_SEGMENT_COUNT: usize = 1000;

// Bulk read size
const BATCH_SIZE: u64 = 10 * 1024 * 1024;

// Number of topics
pub const TOPICS_COUNT: u64 = 100;

// Backup directory
pub const DIR: &str = "/tmp/timestone/storage";

#[derive(FromArgs)]
/// Reach new heights.
struct CommandLine {
    /// size of payload
    #[argh(option, short = 'p', default = "1024")]
    payload_size: usize,
    /// number of messages
    #[argh(option, short = 'n', default = "10*1024*1024")]
    count: usize,
}

fn main() {
    let topics = benchmarks::topics();
    let mut timestone = benchmarks::timestone();
    write_multiple_topics(&mut timestone, &topics);
}

fn write_multiple_topics(log: &mut CommitLog, topics: &Vec<String>) {
    let guard = pprof::ProfilerGuard::new(100).unwrap();
    let start = Instant::now();
    let current_size = benchmarks::write_10k_topics(log, topics);
    benchmarks::report("topicswrite.pb", current_size, start, guard);
}

