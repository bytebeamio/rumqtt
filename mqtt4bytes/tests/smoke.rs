use bytes::{BufMut, BytesMut};
use mqtt4bytes::{mqtt_read, Packet, PubAck, Publish, QoS};
use rand::Rng;

#[test]
fn publish_encode_and_decode_works_as_expected() {
    let mut stream = publishes(1024, 2 * 1024 * 1024);
    let mut pkid = 0;

    // stream which decoder reads from
    let mut read_stream = BytesMut::new();
    loop {
        // fill the decoder stream with n bytes.
        let fill_size = rand::thread_rng().gen_range(0, 1024);
        let len = stream.len();
        let split_len = if len == 0 {
            break;
        } else if len > fill_size {
            fill_size
        } else {
            len
        };

        let bytes = stream.split_to(split_len);
        read_stream.put(bytes);
        let packet = match mqtt_read(&mut read_stream, 10 * 1024) {
            Err(mqtt4bytes::Error::InsufficientBytes(_)) => continue,
            Err(e) => panic!(e),
            Ok(packet) => packet,
        };

        match packet {
            Packet::Publish(publish) => {
                assert_eq!(publish.pkid, (pkid % 65000) + 1);
                pkid = (pkid % 65000) as u16 + 1;
            }
            _ => panic!("Expecting a publish"),
        }
    }
}

pub fn publishes(size: usize, count: usize) -> BytesMut {
    let mut stream = BytesMut::new();

    for i in 0..count {
        let payload = vec![i as u8; size];
        let mut packet = Publish::new("hello/mqtt/topic/bytes", QoS::AtLeastOnce, payload);
        packet.pkid = (i % 65000) as u16 + 1;
        packet.write(&mut stream).unwrap();
    }

    stream
}

#[test]
fn pubacks_encode_and_decode_works_as_expected() {
    let mut stream = pubacks(10 * 1024 * 1024);
    let mut pkid = 0;

    // stream which decoder reads from
    let mut read_stream = BytesMut::new();
    loop {
        // fill the decoder stream with n bytes.
        let fill_size = rand::thread_rng().gen_range(0, 10);
        let len = stream.len();
        let split_len = if len == 0 {
            break;
        } else if len > fill_size {
            fill_size
        } else {
            len
        };

        let bytes = stream.split_to(split_len);
        read_stream.put(bytes);
        let packet = match mqtt_read(&mut read_stream, 10 * 1024) {
            Err(mqtt4bytes::Error::InsufficientBytes(_)) => continue,
            Err(e) => panic!(e),
            Ok(packet) => packet,
        };

        match packet {
            Packet::PubAck(ack) => {
                assert_eq!(ack.pkid, (pkid % 65000) + 1);
                pkid = (pkid % 65000) as u16 + 1;
            }
            _ => panic!("Expecting a publish"),
        }
    }
}

pub fn pubacks(count: usize) -> BytesMut {
    let mut stream = BytesMut::new();

    for i in 0..count {
        let packet = PubAck::new((i % 65000) as u16 + 1);
        packet.write(&mut stream).unwrap();
    }

    stream
}
