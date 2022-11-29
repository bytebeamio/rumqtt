use crate::link::local::{Link, LinkRx};
use crate::router::{Event, MetricsRequest};
use crate::{ConnectionId, ConsoleSettings};
use flume::Sender;
use rouille::{router, try_or_400};
use std::sync::Arc;
use tracing::info;

#[derive(Debug)]
pub struct ConsoleLink {
    config: ConsoleSettings,
    connection_id: ConnectionId,
    router_tx: Sender<(ConnectionId, Event)>,
    _link_rx: LinkRx,
}

impl ConsoleLink {
    /// Requires the corresponding Router to be running to complete
    pub fn new(config: ConsoleSettings, router_tx: Sender<(ConnectionId, Event)>) -> ConsoleLink {
        let tx = router_tx.clone();
        let (link_tx, link_rx, _ack) = Link::new(None, "console", tx, true, None, true).unwrap();
        let connection_id = link_tx.connection_id;
        ConsoleLink {
            config,
            router_tx,
            _link_rx: link_rx,
            connection_id,
        }
    }
}

#[tracing::instrument]
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

                rouille::Response::text("OK").with_status_code(200)
            },
            (GET) (/device/{id: String}) => {
                let event = Event::Metrics(MetricsRequest::Connection(id));
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                rouille::Response::text("OK").with_status_code(200)
            },
            (GET) (/subscriptions) => {
                let event = Event::Metrics(MetricsRequest::Subscriptions);
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                rouille::Response::text("OK").with_status_code(200)
            },
            (GET) (/subscription/{filter: String}) => {
                let filter = filter.replace('.', "/");
                let event = Event::Metrics(MetricsRequest::Subscription(filter));
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                rouille::Response::text("OK").with_status_code(200)
            },
            (GET) (/waiters/{filter: String}) => {
                let filter = filter.replace('.', "/");
                let event = Event::Metrics(MetricsRequest::Waiters(filter));
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                rouille::Response::text("OK").with_status_code(200)
            },
            (GET) (/readyqueue) => {
                let event = Event::Metrics(MetricsRequest::ReadyQueue);
                let message = (console.connection_id, event);
                if console.router_tx.send(message).is_err() {
                    return rouille::Response::empty_404()
                }

                rouille::Response::text("OK").with_status_code(200)
           },
           (POST) (/logs) => {
            info!("Reloading tracing filter");
            let data = try_or_400!(rouille::input::plain_text_body(request));
            if let Some(handle) = &console.config.filter_handle {
                if handle.reload(&data).is_err() {
                    return rouille::Response::empty_400();
                }
                return rouille::Response::text(data);
            }
            rouille::Response::empty_404()
           },
            _ => rouille::Response::empty_404()
        )
    });
}
