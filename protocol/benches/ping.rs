use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let mut buffer = Vec::new();
    c.bench_function("pingreq_write", |b| {
        b.iter(|| {
            (protocol::v4::ping::pingreq::write(&mut buffer)).unwrap();
            buffer.clear();
        })
    });

    let mut buffer = Vec::new();
    c.bench_function("pingresp_write", |b| {
        b.iter(|| {
            let buffer = black_box(&mut buffer);
            protocol::v4::ping::pingresp::write(buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
