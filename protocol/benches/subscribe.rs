use base::messages::{Filter, QoS, RetainForwardRule, Subscribe};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn benchmark(c: &mut Criterion) {
    let packet = Subscribe {
        filters: vec![Filter {
            qos: QoS::AtLeastOnce,
            nolocal: true,
            preserve_retain: true,
            path: "test".to_string(),
            retain_forward_rule: RetainForwardRule::Never,
        }],
        pkid: 0,
        properties: None,
    };
    let mut buffer = Vec::new();
    c.bench_function("subscribe_write", |b| {
        b.iter(|| {
            let (packet, buffer) = black_box((&packet, &mut buffer));
            protocol::v4::subscribe::write(packet, buffer).unwrap();
            buffer.clear();
        })
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
