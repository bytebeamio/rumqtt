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
pub struct Token {}

impl Token {
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
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    Disconnect(Disconnect),
}

impl From<Publish> for Request {
    fn from(publish: Publish) -> Request {
        Request::Publish(publish)
    }
}

impl From<Subscribe> for Request {
    fn from(subscribe: Subscribe) -> Request {
        Request::Subscribe(subscribe)
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
