use tokio::{
    runtime::{Builder, Runtime},
};

use rumq_core::mqtt4::{QoS, Packet, Publish, MqttRead, MqttWrite};

use bencher::{Bencher, black_box, benchmark_group, benchmark_main};

fn publish_serialize_perf(bench: &mut Bencher) {
    let mut runtime = rt();
    let publish = Publish::new("hello/world", QoS::AtLeastOnce, payload(100));
    let publish = Packet::Publish(publish);
    let mut stream = Vec::new();

    bench.iter(|| {
        black_box(runtime.block_on(async {
            stream.mqtt_write(&publish).unwrap();
        }))
    });
}

fn publish_deserialize_perf(bench: &mut Bencher) {
    let mut runtime = rt();
    let mut stream = stream(100);
    
    bench.iter(|| {
        runtime.block_on(async {
            let _packet = stream.mqtt_read().unwrap();
            stream.set_position(0);
        })
    });
}

use std::io::Cursor;
fn stream(len: usize) -> Cursor<Vec<u8>> {
    let mut packets = vec![
        0b00110010, 7 + len as u8,                   // packet type, flags and remaining len
        0x00, 0x03, 'a' as u8, '/' as u8, 'b' as u8, // variable header. topic name = 'a/b'
        0x00, 0x0a,                                  // variable header. pkid = 10
    ];
    
    let mut payload = payload(len);                     // publish payload
    let mut extra = vec![0xDE, 0xAD, 0xBE, 0xEF];       // extra packets in the stream
   
    packets.append(&mut payload);
    packets.append(&mut extra);

    let stream = Cursor::new(packets);
    stream
}

fn rt() -> Runtime {
    Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap()
}




fn payload(count: usize) -> Vec<u8> {
    let mut p = Vec::new();
    for i in 0..count {
        p.push(i as u8)
    }

    p 
} 

benchmark_group!(benches, publish_serialize_perf,  publish_deserialize_perf);
benchmark_main!(benches);
