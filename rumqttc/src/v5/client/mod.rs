//! This module offers a high level synchronous and asynchronous abstraction to
//! async eventloop.
use crate::v5::{packet::*, ConnectionError, EventLoop, Request};

use flume::SendError;
use std::mem;
use tokio::runtime::{self, Runtime};

mod asyncclient;
pub use asyncclient::AsyncClient;
mod syncclient;
pub use syncclient::Client;

/// Client Error
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Failed to send cancel request to eventloop")]
    Cancel(SendError<()>),
    #[error("Failed to send mqtt request to eventloop, the evenloop has been closed")]
    EventloopClosed,
    #[error("Failed to send mqtt request to evenloop, to requests buffer is full right now")]
    RequestsFull,
    #[error("Serialization error")]
    Mqtt5(Error),
}

fn get_ack_req(qos: QoS, pkid: u16) -> Option<Request> {
    let ack = match qos {
        QoS::AtMostOnce => return None,
        QoS::AtLeastOnce => Request::PubAck(PubAck::new(pkid)),
        QoS::ExactlyOnce => Request::PubRec(PubRec::new(pkid)),
    };
    Some(ack)
}

///  MQTT connection. Maintains all the necessary state
pub struct Connection {
    pub eventloop: EventLoop,
    runtime: Option<Runtime>,
}

impl Connection {
    fn new(eventloop: EventLoop, runtime: Runtime) -> Connection {
        Connection {
            eventloop,
            runtime: Some(runtime),
        }
    }

    /// Returns an iterator over this connection. Iterating over this is all that's
    /// necessary to make connection progress and maintain a robust connection.
    /// Just continuing to loop will reconnect
    /// **NOTE** Don't block this while iterating
    #[must_use = "Connection should be iterated over a loop to make progress"]
    pub fn iter(&mut self) -> Iter {
        let runtime = self.runtime.take().unwrap();
        Iter {
            connection: self,
            runtime,
        }
    }
}

/// Iterator which polls the eventloop for connection progress
pub struct Iter<'a> {
    connection: &'a mut Connection,
    runtime: runtime::Runtime,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<(), ConnectionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let f = self.connection.eventloop.poll();
        match self.runtime.block_on(f) {
            Ok(_) => Some(Ok(())),
            // closing of request channel should stop the iterator
            Err(ConnectionError::RequestsDone) => {
                trace!("Done with requests");
                None
            }
            Err(ConnectionError::Cancel) => {
                trace!("Cancellation request received");
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> Drop for Iter<'a> {
    fn drop(&mut self) {
        // TODO: Don't create new runtime in drop
        let runtime = runtime::Builder::new_current_thread().build().unwrap();
        self.connection.runtime = Some(mem::replace(&mut self.runtime, runtime));
    }
}
