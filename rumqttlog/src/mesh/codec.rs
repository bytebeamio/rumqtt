use tokio_util::codec::{LengthDelimitedCodec, Decoder};
use bytes::{BytesMut, Bytes, Buf};
use tokio::io::Error;
use std::io;
use tokio::stream::StreamExt;

pub enum Packet {
    Connect(u8),
    Data(u8, String, Bytes),
    DataAck(u8)
}

pub struct MeshCodec {
    c: LengthDelimitedCodec
}

impl MeshCodec {
    pub fn new() -> MeshCodec {
        let c = LengthDelimitedCodec::builder().num_skip(1).new_codec();
        MeshCodec { c }
    }

    fn packet(b: &mut BytesMut) -> io::Result<Packet> {
        let typ = b.get_u8();
        match typ {
            0 => {
                let id = b.get_u8();
                Ok(Packet::Connect(id))
            }
            1 => {
                let id = b.get_u8();
                let topic_len = b.get_u32();
                let topic = b.split_to(topic_len as usize);
                let topic = String::from_utf8(topic.to_vec()).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Non UTF8 topic"))?;
                let payload_len = b.get_u32();
                let payload = b.split_to(payload_len as usize);
                Ok(Packet::Data(id, topic, payload.freeze()))
            }
            2 => {
                let id = b.get_u8();
                Ok(Packet::DataAck(id))
            }
            _ => {
                Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected packet type"))
            }
        }
    }
}

impl Decoder for MeshCodec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.c.decode(src)? {
            Some(b)  => {
                unimplemented!()
            }
            None => Ok(None),
        }
    }
}


