use base::messages;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = messages::PubAck {
        pkid: 0,
        reason: messages::PubAckReason::Success,
        properties: None,
    };
    let mut buffer = Vec::new();
    c.bench_function("puback_roundtrip", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::puback::write(packet, buffer).unwrap();
            let fixed_header = protocol::v4::parse_fixed_header(buffer).unwrap();
            let _ = protocol::v4::puback::read(fixed_header, buffer).unwrap();
            buffer.clear();
        })
    });

    buffer.clear();
    c.bench_function("puback_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::puback::write(packet, buffer).unwrap();
            buffer.clear();
        });
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
