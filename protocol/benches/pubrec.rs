use base::messages;
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

pub fn benchmark(c: &mut Criterion) {
    let packet = messages::PubRec {
        pkid: 0,
        properties: None,
        reason: messages::PubRecReason::Success,
    };
    let mut buffer = Vec::new();
    c.bench_function("pubrec_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::pubrec::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
