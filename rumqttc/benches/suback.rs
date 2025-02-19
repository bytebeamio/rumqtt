use base::messages::{self, QoS};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = messages::SubAck {
        pkid: 0,
        return_codes: vec![messages::SubscribeReasonCode::Success(QoS::AtLeastOnce)],
        properties: None,
    };
    let mut buffer = Vec::new();
    c.bench_function("suback_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::suback::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
