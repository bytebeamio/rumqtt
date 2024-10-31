pub mod builder;
pub mod client;
pub mod eventloop;
pub mod transport;

pub use base::messages::*;
pub use eventloop::{EventLoop, EventLoopSettings};
pub use transport::TransportSettings;

#[derive(Debug)]
pub enum Notification {
    Message(Publish),
    ManualAckMessage(Publish, Token),
    Event(Event),
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

#[derive(Debug)]
pub enum Event {
    Disconnection,
    Reconnection,
}