use base::messages::{Connect, LastWill, QoS};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use protocol::Protocol;

pub fn get_connect_packet(
    client_id: impl Into<String>,
    topic: impl Into<String>,
    message: impl Into<String>,
) -> Connect {
    Connect {
        keep_alive: 600,
        client_id: client_id.into(),
        clean_session: true,
        login: None,
        last_will: Some(LastWill {
            message: message.into().bytes().collect(),
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: topic.into().bytes().collect(),
        }),
        properties: None,
    }
}

pub fn benchmark(c: &mut Criterion) {
    let packet = get_connect_packet("test-client", "test-topic", "test-message");
    let v4 = protocol::v4::V4;

    c.bench_function("connect/write", |b| {
        b.iter_with_setup(std::vec::Vec::new, |mut buffer| {
            v4.write_connect(&packet, &mut buffer).unwrap();
        })
    });

    let mut group = c.benchmark_group("throughput");

    let number_of_packets = [1, 2, 4, 10, 20];
    for i in number_of_packets {
        let packets = (0..i)
            .map(|i| {
                get_connect_packet(
                    format!("client-{i}"),
                    format!("topic-{i}"),
                    format!("message-{i}"),
                )
            })
            .collect::<Vec<_>>();

        let mut buf = vec![];
        for packet in packets.iter() {
            v4.write_connect(packet, &mut buf).unwrap();
        }

        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_function(BenchmarkId::new("connect/read", i), |b| {
            b.iter(|| {
                let buf = black_box(&buf);
                let bufptr = &mut &buf[..];
                while !bufptr.is_empty() {
                    v4.read_connect(bufptr, bufptr.len()).unwrap();
                }
            })
        });
    }
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
