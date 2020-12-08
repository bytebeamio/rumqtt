// use bytes::{Buf, BufMut, Bytes, BytesMut};
// use std::io;
// use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
//
// #[derive(Debug, Clone)]
// pub enum Packet {
//     Connect(u8),
//     ConnAck,
//     Data(u64, String, Bytes),
//     DataAck(u64),
// }
//
// pub struct MeshCodec {
//     read: LengthDelimitedCodec,
// }
//
// impl MeshCodec {
//     pub fn new() -> MeshCodec {
//         MeshCodec { read: LengthDelimitedCodec::new() }
//     }
//
//     pub fn len(&self, packet: &Packet) -> usize {
//         match packet {
//             Packet::Connect(_) => 2,
//             Packet::ConnAck => 1,
//             Packet::Data(_, topic, payload) => 1 + 8 + topic.len() + payload.len(),
//             Packet::DataAck(_) => 1 + 8,
//         }
//     }
// }
//
// impl Decoder for MeshCodec {
//     type Item = Packet;
//     type Error = io::Error;
//
//     fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
//         match self.read.decode(src)? {
//             Some(mut b) => {
//                 let typ = b.get_u8();
//                 match typ {
//                     1 => Ok(Some(Packet::Connect(b.get_u8()))),
//                     2 => Ok(Some(Packet::ConnAck)),
//                     3 => {
//                         let pkid = b.get_u64();
//                         let topic_len = b.get_u32();
//                         let topic = b.split_to(topic_len as usize);
//                         // We are assuming topics are already utf8 checked by commitlog/connection
//                         let topic = unsafe { String::from_utf8_unchecked(topic.to_vec()) };
//                         let payload_len = b.get_u32();
//                         let payload = b.split_to(payload_len as usize);
//                         let data = Packet::Data(pkid, topic, payload.freeze());
//                         Ok(Some(data))
//                     }
//                     4 => Ok(Some(Packet::DataAck(b.get_u64()))),
//                     _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unrecognized")),
//                 }
//             }
//             None => Ok(None),
//         }
//     }
// }
//
// impl Encoder<Packet> for MeshCodec {
//     type Error = io::Error;
//
//     fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
//         match item {
//             Packet::Connect(id) => Bytes::from(vec![1, id]),
//             Packet::ConnAck => Bytes::from(vec![2]),
//             Packet::Data(pkid, topic, payload) => {
//                 let mut out = BytesMut::from(&[3][..]);
//                 out.put_u64(pkid);
//                 out.put_u32(topic.len() as u32);
//                 out.put_slice(topic.as_bytes());
//                 out.put_u32(payload.len() as u32);
//                 out.put_slice(&payload[..]);
//                 out.freeze()
//             }
//             Packet::DataAck(offset) => {
//                 let mut out = BytesMut::from(&[4][..]);
//                 out.put_u64(offset);
//                 out.freeze()
//             }
//         }
//
//         Ok(())
//     }
// }
