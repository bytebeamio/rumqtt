pub mod builder;
pub mod client;
pub mod eventloop;
pub mod state;
pub mod transport;

pub use base::messages::*;
use base::{EventsTx, Size, XchgPipeA};
pub use eventloop::{EventLoop, EventLoopSettings};
pub use transport::TransportSettings;

#[derive(Debug)]
pub enum Notification {
    Message(Publish),
    ManualAckMessage(Publish, Token),
    Disconnection,
    Reconnection,
}

#[derive(Debug)]
pub struct Token {
    id: usize,
}

impl Token {
    pub fn new(id: usize) -> Self {
        Token { id }
    }

    pub fn wait(&self) -> Result<(), Error> {
        todo!()
    }

    pub async fn resolve(&self) -> Result<(), Error> {
        todo!()
    }
}

/// Events which can be yielded by the event loop
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Incoming(Incoming),
    Outgoing(Outgoing),
}

pub type Incoming = Packet;

/// Current outgoing activity on the eventloop
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outgoing {
    /// Publish packet with packet identifier. 0 implies QoS 0
    Publish(u16),
    /// Subscribe packet with packet identifier
    Subscribe(u16),
    /// Unsubscribe packet with packet identifier
    Unsubscribe(u16),
    /// PubAck packet
    PubAck(u16),
    /// PubRec packet
    PubRec(u16),
    /// PubRel packet
    PubRel(u16),
    /// PubComp packet
    PubComp(u16),
    /// Ping request packet
    PingReq(PingReq),
    /// Ping response packet
    PingResp(PingResp),
    /// Disconnect packet
    Disconnect(Disconnect),
    /// Await for an ack for more outgoing progress
    AwaitAck(u16),
}

/// Requests by the client to mqtt event loop. Request are
/// handled one by one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubComp(PubComp),
    PubRel(PubRel),
    PingReq(PingReq),
    PingResp(PingResp),
    Subscribe(Subscribe, AckSetting),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    Disconnect(Disconnect),
}

impl Size for Request {
    fn size(&self) -> usize {
        match self {
            Request::Publish(publish) => publish.size(),
            Request::Subscribe(subscribe, _) => subscribe.size(),
            Request::PubAck(puback) => puback.size(),
            Request::PubRec(pubrec) => pubrec.size(),
            Request::PubComp(pubcomp) => pubcomp.size(),
            Request::PubRel(pubrel) => pubrel.size(),
            Request::PingReq(_) => 0,
            Request::PingResp(_) => 0,
            Request::SubAck(suback) => suback.size(),
            Request::Unsubscribe(unsubscribe) => unsubscribe.size(),
            Request::UnsubAck(unsuback) => unsuback.size(),
            Request::Disconnect(disconnect) => disconnect.size(),
        }
    }
}

impl From<Publish> for Request {
    fn from(publish: Publish) -> Request {
        Request::Publish(publish)
    }
}

impl From<Unsubscribe> for Request {
    fn from(unsubscribe: Unsubscribe) -> Request {
        Request::Unsubscribe(unsubscribe)
    }
}

pub struct Tx<A, B: Size> {
    pub events_tx: EventsTx<A>,
    pub tx: XchgPipeA<B>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckSetting {
    Auto,
    Manual,
}
