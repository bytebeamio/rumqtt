use crate::{ConnectionError, EventLoop, Incoming, Network, Outgoing};

use tokio::select;
use tokio::stream::StreamExt;

impl EventLoop {
    pub fn set_network(&mut self, network: Network) {
        self.network = Some(network)
    }

    /// Next notification or outgoing request
    pub async fn pollv(&mut self) -> Result<(Vec<Incoming>, Option<Outgoing>), ConnectionError> {
        // This method used to return only incoming network notification while silently looping through
        // outgoing requests. Internal loops inside async functions are risky. Imagine this function
        // with 100 requests and 1 incoming packet. If this `Stream` (which internally loops) is
        // selected with other streams, can potentially do more internal polling (if the socket is ready)
        if self.network.is_none() {
            let connack = self.connect_or_cancel().await?;
            return Ok((vec![connack], None));
        }

        let (incoming, outgoing) = match self.selectv().await {
            Ok((i, o)) => (i, o),
            Err(e) => {
                self.network = None;
                return Err(e);
            }
        };

        Ok((incoming, outgoing))
    }

    /// Select on network and requests and generate keepalive pings when necessary
    async fn selectv(&mut self) -> Result<(Vec<Incoming>, Option<Outgoing>), ConnectionError> {
        let network = &mut self.network.as_mut().unwrap();

        select! {
            // Pull next packet from network
            o = network.readb() => match o {
                Ok(packets) => {
                    return Ok((packets.into(), None))
                }
                Err(e) => return Err(ConnectionError::Io(e))
            },
            // Pull next request from user requests channel.
            // we read user requests only when we are done sending pending
            // packets and inflight queue has space (for flow control)
            o = self.requests_rx.next() => match o {
                Some(request) => {
                    let outgoing = network.write(request).await?;
                    return Ok((Vec::new(), Some(outgoing)))
                }
                None => return Err(ConnectionError::RequestsDone),
            },
            // cancellation requests to stop the polling
            _ = self.cancel_rx.next() => {
                return Err(ConnectionError::Cancel)
            }
        }
    }
}
