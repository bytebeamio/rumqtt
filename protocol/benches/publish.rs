use base::messages::{Publish, QoS};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use protocol::Protocol;

pub fn get_publish_packet(payload: impl Into<String>, topic: impl Into<String>) -> Publish {
    Publish {
        dup: false,
        payload: payload.into().bytes().collect(),
        pkid: 1,
        qos: QoS::ExactlyOnce,
        retain: false,
        topic: topic.into().bytes().collect(),
        properties: None,
    }
}

pub fn benchmark(c: &mut Criterion) {
    let packet = get_publish_packet("sample payload", "sample topic");
    let v4 = protocol::v4::V4;

    c.bench_function("publish/write", |b| {
        b.iter_with_setup(std::vec::Vec::new, |mut buffer| {
            v4.write_publish(&packet, &mut buffer).unwrap();
        })
    });

    let mut group = c.benchmark_group("throughput");

    let number_of_packets = [1, 2, 4, 10, 20];
    for i in number_of_packets {
        let publish_packets = (0..i)
            .map(|i| get_publish_packet(format!("payload-{i}"), format!("topic-{i}")))
            .collect::<Vec<_>>();

        let mut buf = vec![];
        for packet in publish_packets.iter() {
            v4.write_publish(packet, &mut buf).unwrap();
        }

        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_function(BenchmarkId::new("publish/read", i), |b| {
            b.iter(|| {
                let buf = black_box(&buf);
                let bufptr = &mut &buf[..];
                while !bufptr.is_empty() {
                    v4.read_publish(bufptr, bufptr.len()).unwrap();
                }
            })
        });
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
