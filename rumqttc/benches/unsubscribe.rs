use base::messages::Unsubscribe;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = Unsubscribe {
        filters: vec!["foo/bar".to_string()],
        pkid: 0,
        properties: None,
    };
    let mut buffer = Vec::new();
    c.bench_function("unsubscribe_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::unsubscribe::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
