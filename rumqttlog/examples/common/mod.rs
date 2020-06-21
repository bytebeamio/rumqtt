#![allow(warnings)]

use prost::Message;
use rand::Rng;
use std::fs::File;
use std::io::Write;
use std::time::Instant;

use bytes::Bytes;

pub fn generate_payload(payload_size: u64) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let payload: Vec<u8> = (0..payload_size).map(|_| rng.gen_range(0, 255)).collect();
    payload
}

pub fn payloads(size: usize, count: u64) -> Vec<Bytes> {
    let mut out = Vec::new();

    for _i in 0..count {
        let payload = Bytes::from(vec![1; size]);
        out.push(payload)
    }

    out
}

pub fn report(name: &str, size: u64, start: Instant, guard: pprof::ProfilerGuard) {
    let file_size = size / 1024 / 1024;
    let throughput = file_size as u128 * 1000 / start.elapsed().as_millis();
    println!(
        "{}. File size = {}, Throughput = {} MB/s",
        name, file_size, throughput
    );

    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}
