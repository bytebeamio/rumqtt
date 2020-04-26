#![feature(test)]
extern crate test;
use test::Bencher;

use bytes::BytesMut;
use mqtt4bytes::{mqtt_read, mqtt_write, Packet, Publish, QoS};

fn publishes(count: usize, size: usize) -> BytesMut {
    let payload = vec![1; size];
    let mut out = BytesMut::new();
    out.reserve(count * size);

    for _ in 0..count {
        let mut p = Publish::new("hello/mqt4/bytes", QoS::AtLeastOnce, payload.clone());
        p.set_pkid(1);
        mqtt_write(Packet::Publish(p), &mut out).unwrap();
    }

    out
}

#[bench]
fn serialize_publishes(b: &mut Bencher) {
    let payload_size = 1024;
    let payload = vec![1; payload_size];
    let mut out = BytesMut::new();
    b.iter(|| {
        let mut p = Publish::new("hello/mqt4/bytes", QoS::AtLeastOnce, payload.clone());
        p.set_pkid(1);
        mqtt_write(Packet::Publish(p), &mut out).unwrap();
    });

    b.bytes = payload_size as u64;
}

#[bench]
fn deserialize_publishes(b: &mut Bencher) {
    let count = 2 * 1024 * 1024;
    let size = 1024;
    let mut publishes = publishes(count, size);
    b.iter(|| mqtt_read(&mut publishes, 100 * 1024).unwrap());

    b.bytes = size as u64;
}
