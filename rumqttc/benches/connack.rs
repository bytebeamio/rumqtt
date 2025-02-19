use base::messages;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = messages::ConnAck {
        code: messages::ConnectReturnCode::Success,
        properties: None,
        session_present: false,
    };
    let mut buffer = vec![0u8; 1024];
    let mut group = c.benchmark_group("throughput");
    group.throughput(criterion::Throughput::Bytes(buffer.len() as u64));
    group.bench_function("connack_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::connack::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
