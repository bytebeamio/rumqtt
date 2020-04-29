use mqtt4bytes::{Packet, Publish, QoS, mqtt_write, mqtt_read, Error};
use bytes::{Bytes, BytesMut, BufMut};
use rand::Rng;

#[test]
fn encode_and_decode_works_as_expected() {
    let mut stream = packets(1024, 2 * 1024 * 1024);
    let max_read = stream.len();
    let mut total_read = 0;
    let mut pkid = 0;

    // stream which decoder reads from
    let mut read_stream = BytesMut::new();
    loop {
        // done with the stream
        if total_read >=  max_read {
            break
        }

        // fill the decoder stream with n bytes.
        let fill_size = rand::thread_rng().gen_range(0, 1024);
        let len = stream.len();
        let split_len = if len == 0 {
            break
        } else if len > fill_size {
            fill_size
        } else {
            len
        };

        let bytes = stream.split_to(split_len);
        total_read += bytes.len();
        read_stream.put(bytes);
        let packet = match mqtt_read(&mut read_stream, 10 * 1024) {
            Err(mqtt4bytes::Error::UnexpectedEof) => continue,
            Err(e) => panic!(e),
            Ok(packet) => packet,
        };

        match packet {
            Packet::Publish(publish) => {
                assert_eq!(publish.pkid, (pkid % 65000) + 1);
                pkid = (pkid % 65000) as u16 + 1;
            },
            _ => panic!("Expecting a publish")
        }
    }

}

pub fn packets(size: usize, count: usize) -> BytesMut {
    let mut stream = BytesMut::new();

    for i in 0..count {
        let payload = vec![i as u8; size];
        let mut packet = Publish::new("hello/mqtt/topic/bytes", QoS::AtLeastOnce, payload);
        packet.set_pkid((i % 65000) as u16 + 1);
        let packet = Packet::Publish(packet);
        mqtt_write(packet, &mut stream).unwrap();
    }

    stream
}
