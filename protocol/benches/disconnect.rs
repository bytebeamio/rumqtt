use base::messages;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = messages::Disconnect {
        properties: None,
        reason_code: messages::DisconnectReasonCode::NormalDisconnection,
    };
    let mut buffer = Vec::new();
    c.bench_function("disconnect_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::disconnect::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
