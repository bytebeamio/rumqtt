#![feature(test)]
extern crate test;
use test::Bencher;

use rumq_core::mqtt4::{MqttRead, MqttWrite, Packet, Publish, QoS};

#[bench]
fn publish_serialize_perf(bench: &mut Bencher) {
    let publish = Publish::new("hello/world", QoS::AtLeastOnce, payload(1024));
    let publish = Packet::Publish(publish);
    let mut stream = Vec::new();

    bench.iter(|| stream.mqtt_write(&publish).unwrap());
    bench.bytes = 1024;
}

#[bench]
fn publish_deserialize_perf(bench: &mut Bencher) {
    let mut stream = stream(1024);

    bench.iter(|| {
        let _packet = stream.mqtt_read().unwrap();
        stream.set_position(0);
    });
    bench.bytes = 1024;
}

use std::io::Cursor;
fn stream(len: usize) -> Cursor<Vec<u8>> {
    let publish = Publish::new("hello/world", QoS::AtLeastOnce, payload(len));
    let publish = Packet::Publish(publish);
    let mut payload = Vec::new();
    payload.mqtt_write(&publish).unwrap();
    let mut extra = vec![0xDE, 0xAD, 0xBE, 0xEF]; // extra packets in the stream

    payload.append(&mut extra);

    let stream = Cursor::new(payload);
    stream
}

fn payload(count: usize) -> Vec<u8> {
    let mut p = Vec::new();
    for i in 0..count {
        p.push(i as u8)
    }

    p
}
