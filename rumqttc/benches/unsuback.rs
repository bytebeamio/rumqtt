use base::messages::{UnsubAck, UnsubAckReason};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = UnsubAck {
        pkid: 0,
        properties: None,
        reasons: vec![UnsubAckReason::Success],
    };
    let mut buffer = Vec::new();
    c.bench_function("unsuback_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::unsuback::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
