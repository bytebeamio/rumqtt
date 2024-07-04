use std::{error::Error, time::Duration, vec::IntoIter};
use tokio::{task, time};
use tokio_stream::{Iter, StreamExt};

use flume::{bounded, Receiver, Sender};

use rumqttc::{
    v5::{
        mqttbytes::{
            v5::{Filter, Publish, Subscribe},
            QoS,
        },
        AsyncClient, ConnectionError, Event, Incoming, Request,
    },
    Outgoing, PollableEventLoop,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    #[rustfmt::skip]
    let exp_subs = vec![Subscribe::new(
        Filter::new("hello/world", QoS::AtMostOnce),
        None,
    )];

    #[rustfmt::skip]
    let exp_pubs = vec![
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1, 1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1, 1, 1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1, 1, 1, 1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1, 1, 1, 1, 1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1, 1, 1, 1, 1, 1, 1], None),
        Publish::new("hello/world", QoS::ExactlyOnce, vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1], None),
    ];

    let (client, mut eventloop) = EventLoopMock::new_async_client_mock(exp_subs, exp_pubs);

    task::spawn(async move {
        requests(client).await;
        time::sleep(Duration::from_secs(2)).await;
    });

    loop {
        let event = &eventloop.poll().await;
        match &event {
            Ok(ev) => {
                println!("Event = {ev:?}");
                if let Event::Outgoing(Outgoing::Disconnect) = ev {
                    break;
                }
            }
            Err(e) => {
                println!("Error = {e:?}");
                break;
            }
        }
    }

    eventloop.assert_expectations();

    Ok(())
}

async fn requests(client: AsyncClient) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .publish("hello/world", QoS::ExactlyOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(25)).await;
    _ = client.disconnect().await;
}

/// Simple non-elaborated example of a mocked EventLoop.
/// You should use your favourite mocking library for proper mocking
pub struct EventLoopMock {
    /// Request stream
    requests_rx: Receiver<Request>,
    /// Requests handle to send requests
    pub(crate) _requests_tx: Sender<Request>,
    /// saved subscriptions for comparison
    expected_subs: Vec<Subscribe>,
    /// saved publications for comparison
    expected_pubs: Vec<Publish>,
    /// saved subscriptions for comparison
    saved_subs: Vec<Subscribe>,
    /// saved publications for comparison
    saved_pubs: Vec<Publish>,
    /// async iterator for simulated incoming events
    sim_incoming: Iter<IntoIter<Publish>>,
}

impl EventLoopMock {
    #[must_use]
    pub fn new_async_client_mock(
        expected_subs: Vec<Subscribe>,
        expected_pubs: Vec<Publish>,
    ) -> (AsyncClient, EventLoopMock) {
        let (requests_tx, requests_rx) = bounded(5);
        let client = AsyncClient::from_senders(requests_tx.clone());
        let sim_incoming = tokio_stream::iter(expected_pubs.clone());
        let eventloop = EventLoopMock {
            requests_rx,
            _requests_tx: requests_tx,
            expected_subs,
            expected_pubs,
            saved_subs: Vec::new(),
            saved_pubs: Vec::new(),
            sim_incoming,
        };
        (client, eventloop)
    }

    /// assert if expectations met
    pub fn assert_expectations(&self) {
        // dbg!(&self.expected_subs);
        // dbg!(&self.saved_subs);
        assert_eq!(self.expected_subs.iter().eq(self.saved_subs.iter()), true);

        // dbg!(&self.expected_pubs);
        // dbg!(&self.saved_pubs);
        assert_eq!(self.expected_pubs.iter().eq(self.saved_pubs.iter()), true);

        println!("assertions passed");
    }
}

#[allow(refining_impl_trait)]
impl PollableEventLoop for EventLoopMock {
    type Event = rumqttc::v5::Event;
    type ConnectionError = rumqttc::v5::ConnectionError;

    async fn poll(&mut self) -> Result<Self::Event, Self::ConnectionError> {
        tokio::select! {
            out_request = self.requests_rx.recv_async() => {
                match out_request {
                    Ok(out_req) => {
                        match out_req {
                            Request::Publish(p) => {
                                println!("pub {p:?}");
                                self.saved_pubs.push(p);
                                return Ok(Event::Outgoing(Outgoing::Publish(0))) // return fake event, will be ignored here
                            },
                            Request::Subscribe(s) => {
                                println!("sub {s:?}");
                                self.saved_subs.push(s);
                                return Ok(Event::Outgoing(Outgoing::Subscribe(0))) // return fake event, will be ignored here
                            },
                            Request::Disconnect => {
                                println!("disconn");
                                return Ok(Event::Outgoing(Outgoing::Disconnect)) // shutdown eventloop
                            }
                            _ => {
                                println!("Unknown {out_req:?}");
                                return Err(ConnectionError::RequestsDone)
                            }
                        }
                    },
                    Err(_) => {
                        return Err(ConnectionError::RequestsDone)
                    }
                }

            }
            _ = tokio::time::sleep(Duration::from_millis(1200)) => {
                // delayed, so it will only trigger when no outgoing event
                match self.sim_incoming.next().await {
                    Some(ev) => return Ok(Event::Incoming(Incoming::Publish(ev))),
                    None => return Err(ConnectionError::RequestsDone),
                }
            }
        }
    }
}
