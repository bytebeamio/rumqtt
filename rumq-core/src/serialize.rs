use crate::{Error, Packet, QoS, SubscribeReturnCodes, SubscribeTopic};
use async_trait::async_trait;

use tokio_byteorder::{BigEndian, AsyncWriteBytesExt};
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait MqttWrite: AsyncWriteBytesExt + Unpin {
    async fn mqtt_write(&mut self, packet: &Packet) -> Result<(), Error> {
        match packet {
            Packet::Connect(connect) => {
                self.write_u8(0b00010000).await?;
                self.write_remaining_length(connect.len()).await?;
                self.write_mqtt_string("MQTT").await?;
                self.write_u8(0x04).await?;

                let mut connect_flags = 0;
                if connect.clean_session {
                    connect_flags |= 0x02;
                }

                match &connect.last_will {
                    Some(w) if w.retain => connect_flags |= 0x04 | (w.qos as u8) << 3 | 0x20,
                    Some(w) => connect_flags |= 0x04 | (w.qos as u8) << 3,
                    None => ()
                }

                if let Some(_) = connect.password {
                    connect_flags |= 0x40;
                }
                if let Some(_) = connect.username {
                    connect_flags |= 0x80;
                }

                self.write_u8(connect_flags).await?;
                self.write_u16::<BigEndian>(connect.keep_alive).await?;
                self.write_mqtt_string(connect.client_id.as_ref()).await?;

                if let Some(ref last_will) = connect.last_will {
                    self.write_mqtt_string(last_will.topic.as_ref()).await?;
                    self.write_mqtt_string(last_will.message.as_ref()).await?;
                }
                if let Some(ref username) = connect.username {
                    self.write_mqtt_string(username).await?;
                }
                if let Some(ref password) = connect.password {
                    self.write_mqtt_string(password).await?;
                }
                Ok(())
            }
            Packet::Connack(connack) => {
                let session_present = connack.session_present as u8;
                let code = connack.code as u8;
                let data = [0x20, 0x02, session_present, code];
                self.write_all(&data).await?;
                Ok(())
            }
            Packet::Publish(publish) => {
                self.write_u8(0b00110000 | publish.retain as u8 | ((publish.qos as u8) << 1) | ((publish.dup as u8) << 3)).await?;
                let mut len = publish.topic_name.len() + 2 + publish.payload.len();

                if publish.qos != QoS::AtMostOnce && None != publish.pkid {
                    len += 2;
                }

                self.write_remaining_length(len).await?;
                self.write_mqtt_string(publish.topic_name.as_str()).await?;
                if publish.qos != QoS::AtMostOnce {
                    if let Some(pkid) = publish.pkid {
                        self.write_u16::<BigEndian>(pkid.0).await?;
                    }
                }
                self.write_all(&publish.payload.as_ref()).await?;
                Ok(())
            }
            Packet::Puback(pkid) => {
                self.write_all(&[0x40, 0x02]).await?;
                self.write_u16::<BigEndian>(pkid.0).await?;
                Ok(())
            }
            Packet::Pubrec(pkid) => {
                self.write_all(&[0x50, 0x02]).await?;
                self.write_u16::<BigEndian>(pkid.0).await?;
                Ok(())
            }
            Packet::Pubrel(pkid) => {
                self.write_all(&[0x62, 0x02]).await?;
                self.write_u16::<BigEndian>(pkid.0).await?;
                Ok(())
            }
            Packet::Pubcomp(pkid) => {
                self.write_all(&[0x70, 0x02]).await?;
                self.write_u16::<BigEndian>(pkid.0).await?;
                Ok(())
            }
            Packet::Subscribe(subscribe) => {
                self.write_all(&[0x82]).await?;
                let len = 2 + subscribe.topics.iter().fold(0, |s, ref t| s + t.topic_path.len() + 3);
                
                self.write_remaining_length(len).await?;
                self.write_u16::<BigEndian>(subscribe.pkid.0).await?;
                for topic in subscribe.topics.as_ref() as &Vec<SubscribeTopic> {
                    self.write_mqtt_string(topic.topic_path.as_str()).await?;
                    self.write_u8(topic.qos as u8).await?;
                }
                Ok(())
            }
            Packet::Suback(suback) => {
                self.write_all(&[0x90]).await?;
                self.write_remaining_length(suback.return_codes.len() + 2).await?;
                self.write_u16::<BigEndian>(suback.pkid.0).await?;
                
                let payload: Vec<u8> = suback.return_codes.iter().map(|&code| match code {
                    SubscribeReturnCodes::Success(qos) => qos as u8,
                    SubscribeReturnCodes::Failure => 0x80,
                }).collect();
                
                self.write_all(&payload).await?;
                Ok(())
            }
            Packet::Unsubscribe(unsubscribe) => {
                self.write_all(&[0xA2]).await?;
                let len = 2 + unsubscribe.topics.iter().fold(0, |s, ref topic| s + topic.len() + 2);
                self.write_remaining_length(len).await?;
                self.write_u16::<BigEndian>(unsubscribe.pkid.0).await?;
                
                for topic in unsubscribe.topics.as_ref() as &Vec<String> {
                    self.write_mqtt_string(topic.as_str()).await?;
                }
                Ok(())
            }
            Packet::Unsuback(pkid) => {
                self.write_all(&[0xB0, 0x02]).await?;
                self.write_u16::<BigEndian>(pkid.0).await?;
                Ok(())
            }
            Packet::Pingreq => {
                self.write_all(&[0xc0, 0]).await?;
                Ok(())
            }
            Packet::Pingresp => {
                self.write_all(&[0xd0, 0]).await?;
                Ok(())
            }
            Packet::Disconnect => {
                self.write_all(&[0xe0, 0]).await?;
                Ok(())
            }
        }
    }

    async fn write_mqtt_string(&mut self, string: &str) -> Result<(), Error> {
        self.write_u16::<BigEndian>(string.len() as u16).await?;
        self.write_all(string.as_bytes()).await?;
        Ok(())
    }

    async fn write_remaining_length(&mut self, len: usize) -> Result<(), Error> {
        if len > 268_435_455 {
            return Err(Error::PayloadTooLong);
        }

        let mut done = false;
        let mut x = len;

        while !done {
            let mut byte = (x % 128) as u8;
            x = x / 128;
            if x > 0 {
                byte = byte | 128;
            }
            self.write_u8(byte).await?;
            done = x <= 0;
        }
        Ok(())
    }
}

/// Implement MqttWrite for every AsyncWriteBytesExt type (and hence AsyncWrite type)
impl<W: AsyncWriteBytesExt + ?Sized + Unpin> MqttWrite for W {}

#[cfg(test)]
mod test {
    use super::MqttWrite;
    use crate::{Connack, Connect, Packet, Publish, Subscribe};
    use crate::{ConnectReturnCode, LastWill, PacketIdentifier, Protocol, QoS, SubscribeTopic};
    use std::sync::Arc;

    #[tokio::test]
    async fn write_packet_connect_mqtt_protocol_works() {
        let connect = Packet::Connect(Connect {
            protocol: Protocol::MQTT(4),
            keep_alive: 10,
            client_id: "test".to_owned(),
            clean_session: true,
            last_will: Some(LastWill {
                topic: "/a".to_owned(),
                message: "offline".to_owned(),
                retain: false,
                qos: QoS::AtLeastOnce,
            }),
            username: Some("rust".to_owned()),
            password: Some("mq".to_owned()),
        });

        let mut stream = Vec::new();
        stream.mqtt_write(&connect).await.unwrap();

        assert_eq!(
            stream.clone(),
            vec![
                0x10, 39, 0x00, 0x04, 'M' as u8, 'Q' as u8, 'T' as u8, 'T' as u8, 0x04,
                0b11001110, // +username, +password, -will retain, will qos=1, +last_will, +clean_session
                0x00, 0x0a, // 10 sec
                0x00, 0x04, 't' as u8, 'e' as u8, 's' as u8, 't' as u8, // client_id
                0x00, 0x02, '/' as u8, 'a' as u8, // will topic = '/a'
                0x00, 0x07, 'o' as u8, 'f' as u8, 'f' as u8, 'l' as u8, 'i' as u8, 'n' as u8, 'e' as u8, // will msg = 'offline'
                0x00, 0x04, 'r' as u8, 'u' as u8, 's' as u8, 't' as u8, // username = 'rust'
                0x00, 0x02, 'm' as u8, 'q' as u8 // password = 'mq'
            ]
        );
    }

    #[tokio::test]
    async fn write_packet_connack_works() {
        let connack = Packet::Connack(Connack {
            session_present: true,
            code: ConnectReturnCode::Accepted,
        });

        let mut stream = Vec::new();
        stream.mqtt_write(&connack).await.unwrap();

        assert_eq!(stream, vec![0b00100000, 0x02, 0x01, 0x00]);
    }

    #[tokio::test]
    async fn write_packet_publish_at_least_once_works() {
        let publish = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic_name: "a/b".to_owned(),
            pkid: Some(PacketIdentifier(10)),
            payload: Arc::new(vec![0xF1, 0xF2, 0xF3, 0xF4]),
        });

        let mut stream = Vec::new();
        stream.mqtt_write(&publish).await.unwrap();

        assert_eq!(
            stream,
            vec![0b00110010, 11, 0x00, 0x03, 'a' as u8, '/' as u8, 'b' as u8, 0x00, 0x0a, 0xF1, 0xF2, 0xF3, 0xF4]
        );
    }

    #[tokio::test]
    async fn write_packet_publish_at_most_once_works() {
        let publish = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic_name: "a/b".to_owned(),
            pkid: None,
            payload: Arc::new(vec![0xE1, 0xE2, 0xE3, 0xE4]),
        });

        let mut stream = Vec::new();
        stream.mqtt_write(&publish).await.unwrap();

        assert_eq!(
            stream,
            vec![0b00110000, 9, 0x00, 0x03, 'a' as u8, '/' as u8, 'b' as u8, 0xE1, 0xE2, 0xE3, 0xE4]
        );
    }

    #[tokio::test]
    async fn write_packet_subscribe_works() {
        let subscribe = Packet::Subscribe(Subscribe {
            pkid: PacketIdentifier(260),
            topics: vec![
                SubscribeTopic {
                    topic_path: "a/+".to_owned(),
                    qos: QoS::AtMostOnce,
                },
                SubscribeTopic {
                    topic_path: "#".to_owned(),
                    qos: QoS::AtLeastOnce,
                },
                SubscribeTopic {
                    topic_path: "a/b/c".to_owned(),
                    qos: QoS::ExactlyOnce,
                },
            ],
        });

        let mut stream = Vec::new();
        stream.mqtt_write(&subscribe).await.unwrap();

        assert_eq!(
            stream,
            vec![
                0b10000010, 20, 0x01, 0x04, // pkid = 260
                0x00, 0x03, 'a' as u8, '/' as u8, '+' as u8, // topic filter = 'a/+'
                0x00,      // qos = 0
                0x00, 0x01, '#' as u8, // topic filter = '#'
                0x01,      // qos = 1
                0x00, 0x05, 'a' as u8, '/' as u8, 'b' as u8, '/' as u8, 'c' as u8, // topic filter = 'a/b/c'
                0x02       // qos = 2
            ]
        );
    }
}
