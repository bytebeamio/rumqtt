/*
#[macro_use]
extern crate bencher;

use bencher::Bencher;
use bytes::Bytes;
use rumqttlog::volatile::Log;

fn payloads(size: usize, count: u64) -> Vec<Bytes> {
    let mut out = Vec::new();

    for _i in 0..count {
        let payload = Bytes::from(vec![1; size]);
        out.push(payload)
    }

    out
}

fn receive_and_store(b: &mut Bencher) {
    let payload_size = 1024;
    let mut payloads = payloads(payload_size, 1_000_000).into_iter();
    let mut pkid = 0;
    let mut log = Log::new(500 * 1024, 10000).unwrap();
    b.iter(|| {
        pkid += 1;
        log.append(pkid, payloads.next().unwrap()).unwrap();
    });

    // println!("pkid = {}, current segment = {}", pkid, log.active_chunk());
    b.bytes = payload_size as u64
}

fn read(b: &mut Bencher) {
    let payload_size = 1024;
    let count = 1_000_000;
    let mut payloads = payloads(payload_size, count).into_iter();
    let mut log = Log::new(500 * 1024, 10000).unwrap();
    for pkid in 0..count {
        let pkid = pkid % 65000;
        log.append(pkid as u16, payloads.next().unwrap()).unwrap();
    }

    let mut segment = 0;
    let mut offset = 0;
    let mut iterations = 0;
    let read_size = 10 * 1024;
    b.iter(|| {
        iterations += 1;
        let (s, o, _, _, _data) = log.readv(segment, offset, read_size).unwrap();
        segment = s;
        offset = o + 1;
    });

    // println!("segment = {}, offset = {}", segment, offset);
    b.bytes = read_size as u64;
}

benchmark_group!(benches, receive_and_store, read);
benchmark_main!(benches);
 */

fn main() {}
