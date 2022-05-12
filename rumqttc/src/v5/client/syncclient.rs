use tokio::runtime;

use crate::v5::{
    client::get_ack_req,
    packet::{Publish, Subscribe, SubscribeFilter, Unsubscribe},
    AsyncClient, ClientError, Connection, MqttOptions, QoS, Request,
};

/// `Client` to communicate with MQTT eventloop `Connection`.
///
/// Client is cloneable and can be used to synchronously Publish, Subscribe.
/// Asynchronous channel handle can also be extracted if necessary
pub struct Client {
    client: AsyncClient,
}

impl Client {
    /// Create a new `Client`
    pub fn new(options: MqttOptions, cap: usize) -> (Client, Connection) {
        let (client, eventloop) = AsyncClient::new(options, cap);
        let client = Client { client };
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let connection = Connection::new(eventloop, runtime);
        (client, connection)
    }

    /// Sends a MQTT Publish to the eventloop
    pub fn publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        let mut publish = Publish::new(topic, qos, payload);
        publish.retain = retain;
        let pkid = if qos != QoS::AtMostOnce {
            let mut request_buf = self.client.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            publish.pkid = pkid;
            request_buf.buf.push_back(Request::Publish(publish));
            pkid
        } else {
            0
        };
        self.client.notify()?;
        Ok(pkid)
    }

    pub fn try_publish<S, V>(
        &self,
        topic: S,
        qos: QoS,
        retain: bool,
        payload: V,
    ) -> Result<u16, ClientError>
    where
        S: Into<String>,
        V: Into<Vec<u8>>,
    {
        self.client.try_publish(topic, qos, retain, payload)
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn ack(&self, publish: &Publish) -> Result<(), ClientError> {
        if let Some(ack) = get_ack_req(publish.qos, publish.pkid) {
            let mut request_buf = self.client.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            request_buf.buf.push_back(ack);
            self.client.notify()?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        self.client.try_ack(publish)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<u16, ClientError> {
        let mut subscribe = Subscribe::new(topic.into(), qos);
        let pkid = if qos != QoS::AtMostOnce {
            let mut request_buf = self.client.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            subscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Subscribe(subscribe));
            pkid
        } else {
            0
        };
        self.client.notify()?;
        Ok(pkid)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(&self, topic: S, qos: QoS) -> Result<u16, ClientError> {
        self.client.try_subscribe(topic, qos)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn subscribe_many<T>(&self, topics: T) -> Result<u16, ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let mut subscribe = Subscribe::new_many(topics);
        let pkid = {
            let mut request_buf = self.client.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            subscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Subscribe(subscribe));
            pkid
        };
        self.client.notify()?;
        Ok(pkid)
    }

    pub fn try_subscribe_many<T>(&self, topics: T) -> Result<u16, ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        self.client.try_subscribe_many(topics)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn unsubscribe<S: Into<String>>(&self, topic: S) -> Result<u16, ClientError> {
        let mut unsubscribe = Unsubscribe::new(topic.into());
        let pkid = {
            let mut request_buf = self.client.outgoing_buf.lock().unwrap();
            if request_buf.buf.len() == request_buf.capacity {
                return Err(ClientError::RequestsFull);
            }
            let pkid = request_buf.increment_pkid();
            unsubscribe.pkid = pkid;
            request_buf.buf.push_back(Request::Unsubscribe(unsubscribe));
            pkid
        };
        self.client.notify()?;
        Ok(pkid)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&self, topic: S) -> Result<u16, ClientError> {
        self.client.try_unsubscribe(topic)
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn disconnect(&self) -> Result<(), ClientError> {
        let mut request_buf = self.client.outgoing_buf.lock().unwrap();
        if request_buf.buf.len() == request_buf.capacity {
            return Err(ClientError::RequestsFull);
        }
        request_buf.buf.push_back(Request::Disconnect);
        self.client.notify()
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&self) -> Result<(), ClientError> {
        self.client.try_disconnect()
    }
}
