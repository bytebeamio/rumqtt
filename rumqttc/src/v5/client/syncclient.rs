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
#[derive(Clone)]
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
        &mut self,
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
        let pkid = publish.pkid;
        self.client.push_and_notify(Request::Publish(publish))?;
        Ok(pkid)
    }

    pub fn try_publish<S, V>(
        &mut self,
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
            self.client.push_and_notify(ack)?;
        }
        Ok(())
    }

    /// Sends a MQTT PubAck to the eventloop. Only needed in if `manual_acks` flag is set.
    pub fn try_ack(&self, publish: &Publish) -> Result<(), ClientError> {
        self.client.try_ack(publish)
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn subscribe<S: Into<String>>(&mut self, topic: S, qos: QoS) -> Result<(), ClientError> {
        let subscribe = Subscribe::new(topic.into(), qos);
        self.client.push_and_notify(Request::Subscribe(subscribe))
    }

    /// Sends a MQTT Subscribe to the eventloop
    pub fn try_subscribe<S: Into<String>>(
        &mut self,
        topic: S,
        qos: QoS,
    ) -> Result<(), ClientError> {
        self.client.try_subscribe(topic, qos)
    }

    /// Sends a MQTT Subscribe for multiple topics to the eventloop
    pub fn subscribe_many<T>(&mut self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        let subscribe = Subscribe::new_many(topics);
        self.client.push_and_notify(Request::Subscribe(subscribe))
    }

    pub fn try_subscribe_many<T>(&mut self, topics: T) -> Result<(), ClientError>
    where
        T: IntoIterator<Item = SubscribeFilter>,
    {
        self.client.try_subscribe_many(topics)
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<(), ClientError> {
        let unsubscribe = Unsubscribe::new(topic.into());
        self.client
            .push_and_notify(Request::Unsubscribe(unsubscribe))
    }

    /// Sends a MQTT Unsubscribe to the eventloop
    pub fn try_unsubscribe<S: Into<String>>(&mut self, topic: S) -> Result<(), ClientError> {
        self.client.try_unsubscribe(topic)
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn disconnect(&mut self) -> Result<(), ClientError> {
        self.client.push_and_notify(Request::Disconnect)
    }

    /// Sends a MQTT disconnect to the eventloop
    pub fn try_disconnect(&mut self) -> Result<(), ClientError> {
        self.client.try_disconnect()
    }
}
