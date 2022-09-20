pub mod local;
pub mod remote;

#[derive(Debug)]
pub enum Error {
    // Mqtt(String),
}

// // Deserializes payload in publish packet into offset + a list of publishes
// pub fn replica_read(_stream: &mut Bytes) -> Result<ReplicaData, Error> {
//     let packet = read(stream, 1024 * 1024 * 1024).map_err(|v| Error::Mqtt(v.to_string()))?;
//     match packet {
//         Packet::Publish(publish) => {
//             let mut payload = publish.payload;
//             let topic = publish.topic;
//             match topic.as_ref() {
//                 "data" => {
//                     let cursor = (payload.get_u64(), payload.get_u64());
//                     Ok(ReplicaData::Data {
//                         cursor,
//                         publishes: payload,
//                     })
//                 }
//                 "acks" => {
//                     let offset = (payload.get_u64(), payload.get_u64());
//                     Ok(ReplicaData::Ack(offset))
//                 }
//                 v => unreachable!("Replica data type = {}", v),
//             }
//         }
//         Packet::Subscribe(_) => Ok(ReplicaData::Subscribe),
//         incoming => unreachable!("Replica only supports publish packet. Received = {:?}", incoming),
//     }
// }

// #[derive(Debug)]
// pub enum ReplicaData {
//     Data {
//         cursor: (u64, u64),
//         publishes: Bytes,
//     },
//     Ack((u64, u64)),
//     Subscribe,
// }

// Reads a stream of bytes and extracts next MQTT packet out of it
// pub fn read_publish(stream: &mut Bytes) -> Result<Publish, v4::Error> {
//     let fixed_header = check(stream.iter(), 1024 * 1024 * 1024)?;

//     // Test with a stream with exactly the size to check border panics
//     let packet = stream.split_to(fixed_header.frame_length());
//     let packet_type = fixed_header.packet_type()?;

//     let publish = match packet_type {
//         PacketType::Publish => Publish::read(fixed_header, packet)?,
//         v => {
//             unreachable!(
//                 "Replica deserialization is only for a list of publishes. Received = {:?}",
//                 v
//             )
//         }
//     };

//     Ok(publish)
// }
