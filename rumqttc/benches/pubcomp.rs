use base::messages;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = messages::PubComp {
        pkid: 0,
        reason: messages::PubCompReason::Success,
        properties: None,
    };
    let mut buffer = Vec::new();
    c.bench_function("pubcomp_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::pubcomp::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
