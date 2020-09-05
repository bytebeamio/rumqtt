use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use mqtt4bytes::{mqtt_read, Publish, QoS};

fn publishes(count: usize, size: usize) -> BytesMut {
    let payload = vec![1; size];
    let mut out = BytesMut::with_capacity(2 * count * size);

    for _ in 0..count {
        let mut p = Publish::new("hello/mqt4/bytes", QoS::AtLeastOnce, payload.clone());
        p.set_pkid(1);
        p.write(&mut out).unwrap();
    }

    out
}

fn publish(payload_size: usize) -> (Publish, BytesMut) {
    let payload = vec![1; payload_size];
    let mut publish = Publish::new("hello/mqt4/bytes", QoS::AtLeastOnce, payload);
    publish.set_pkid(1);

    let out = BytesMut::with_capacity(2 * payload_size);
    (publish, out)
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode and decode throughput");
    group.throughput(Throughput::Bytes(1 * 1024));
    group.bench_function("decode 1 packet, 1024 bytes", move |b| {
        b.iter_batched(
            || publishes(1, 1024),
            |mut stream| black_box(mqtt_read(&mut stream, 2 * 1024).unwrap()),
            BatchSize::SmallInput,
        )
    });

    group.throughput(Throughput::Bytes(1 * 1024));
    group.bench_function("encode 1 packet, 1024 bytes", move |b| {
        b.iter_batched(
            || publish(1024),
            |(publish, mut out)| black_box(publish.write(&mut out).unwrap()),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
