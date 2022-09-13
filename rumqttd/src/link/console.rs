use crate::link::local::{Link, LinkRx};
use crate::router::{Event, MetricsRequest};
use crate::{ConnectionId, ConsoleSettings};
use flume::Sender;
use std::sync::Arc;

pub struct ConsoleLink {
    config: ConsoleSettings,
    connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    link_rx: LinkRx,
}

impl ConsoleLink {
    /// Requires the corresponding Router to be running to complete
    pub fn new(config: ConsoleSettings, router_tx: Sender<(ConnectionId, Event)>) -> ConsoleLink {
        let tx = router_tx.clone();
        let (link_tx, link_rx, _ack) =
            Link::new(None, "console", tx.clone(), true, None, true).unwrap();
        let connection_id = link_tx.connection_id;
        ConsoleLink {
            config,
            router_tx,
            link_rx,
            connection_id,
        }
    }
}

pub fn start(console: Arc<ConsoleLink>) {
    let address = console.config.listen.clone();

    rouille::start_server(address, move |request| {
        router!(request,
            (GET) (/) => {
                rouille::Response::redirect_302("/config")
            },
            (GET) (/config) => {
                rouille::Response::json(&console.config.clone())
            },
            (GET) (/router) => {
                let event = Event::Metrics(MetricsRequest::Router);
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                let v = console.link_rx.metrics();
                rouille::Response::json(&v)
            },
            (GET) (/device/{id: String}) => {
                let event = Event::Metrics(MetricsRequest::Connection(id));
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                let v = console.link_rx.metrics();
                rouille::Response::json(&v)
            },
            (GET) (/subscriptions) => {
                let event = Event::Metrics(MetricsRequest::Subscriptions);
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                let v = console.link_rx.metrics();
                rouille::Response::json(&v)
            },
            (GET) (/subscription/{filter: String}) => {
                let filter = filter.replace(".", "/");
                let event = Event::Metrics(MetricsRequest::Subscription(filter));
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                let v = console.link_rx.metrics();
                rouille::Response::json(&v)
            },
            (GET) (/waiters/{filter: String}) => {
                let filter = filter.replace(".", "/");
                let event = Event::Metrics(MetricsRequest::Waiters(filter));
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                let v = console.link_rx.metrics();
                rouille::Response::json(&v)
            },
            (GET) (/readyqueue) => {
                let event = Event::Metrics(MetricsRequest::ReadyQueue);
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                let v = console.link_rx.metrics();
                rouille::Response::json(&v)
           },
            _ => rouille::Response::empty_404()
        )
    });
}
