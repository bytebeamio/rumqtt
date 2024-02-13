use crate::link::local::LinkRx;
use crate::local::LinkBuilder;
use crate::router::{Event, Print};
use crate::{ConnectionId, ConsoleSettings};
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Json;
use axum::{routing::get, Router};
use flume::Sender;
use std::net::TcpListener;
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
        let (link_tx, link_rx, _ack) = LinkBuilder::new("console", tx)
            .dynamic_filters(true)
            .build()
            .unwrap();
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
pub async fn start(console: Arc<ConsoleLink>) {
    let listener = TcpListener::bind(console.config.listen.clone()).unwrap();

    let app = Router::new()
        .route("/", get(root))
        .route("/config", get(config))
        .route("/router", get(router))
        .route("/device/:device_id", get(device_with_id))
        .route("/subscriptions", get(subscriptions))
        .route("/subscriptions/:filter", get(subscriptions_with_filter))
        .route("/waiters/:filter", get(waiters_with_filter))
        .route("/readyqueue", get(readyqueue))
        .route("/logs", post(logs))
        .with_state(console);

    axum::Server::from_tcp(listener)
        .unwrap()
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn root(State(console): State<Arc<ConsoleLink>>) -> impl IntoResponse {
    Json(console.config.clone())
}

async fn config(State(console): State<Arc<ConsoleLink>>) -> impl IntoResponse {
    let event = Event::PrintStatus(Print::Config);
    let message = (console.connection_id, event);
    if console.router_tx.send(message).is_err() {
        return Response::builder().status(404).body("".to_owned()).unwrap();
    }

    Response::new("OK".to_owned())
}

async fn router(State(console): State<Arc<ConsoleLink>>) -> impl IntoResponse {
    let event = Event::PrintStatus(Print::Router);
    let message = (console.connection_id, event);
    if console.router_tx.send(message).is_err() {
        return Response::builder().status(404).body("".to_owned()).unwrap();
    }

    Response::new("OK".to_owned())
}

async fn device_with_id(
    Path(device_id): Path<String>,
    State(console): State<Arc<ConsoleLink>>,
) -> impl IntoResponse {
    let event = Event::PrintStatus(Print::Connection(device_id));
    let message = (console.connection_id, event);
    if console.router_tx.send(message).is_err() {
        return Response::builder().status(404).body("".to_owned()).unwrap();
    }

    Response::new("OK".to_owned())
}

async fn subscriptions(State(console): State<Arc<ConsoleLink>>) -> impl IntoResponse {
    let event = Event::PrintStatus(Print::Subscriptions);
    let message = (console.connection_id, event);
    if console.router_tx.send(message).is_err() {
        return Response::builder().status(404).body("".to_owned()).unwrap();
    }

    Response::new("OK".to_owned())
}

async fn subscriptions_with_filter(
    Path(filter): Path<String>,
    State(console): State<Arc<ConsoleLink>>,
) -> impl IntoResponse {
    let filter = filter.replace('.', "/");
    let event = Event::PrintStatus(Print::Subscription(filter));
    let message = (console.connection_id, event);
    if console.router_tx.send(message).is_err() {
        return Response::builder().status(404).body("".to_owned()).unwrap();
    }

    Response::new("OK".to_owned())
}

async fn waiters_with_filter(
    Path(filter): Path<String>,
    State(console): State<Arc<ConsoleLink>>,
) -> impl IntoResponse {
    let filter = filter.replace('.', "/");
    let event = Event::PrintStatus(Print::Waiters(filter));
    let message = (console.connection_id, event);
    if console.router_tx.send(message).is_err() {
        return Response::builder().status(404).body("".to_owned()).unwrap();
    }

    Response::new("OK".to_owned())
}

async fn readyqueue(State(console): State<Arc<ConsoleLink>>) -> impl IntoResponse {
    let event = Event::PrintStatus(Print::ReadyQueue);
    let message = (console.connection_id, event);
    if console.router_tx.send(message).is_err() {
        return Response::builder().status(404).body("".to_owned()).unwrap();
    }

    Response::new("OK".to_owned())
}

async fn logs(State(console): State<Arc<ConsoleLink>>, data: String) -> impl IntoResponse {
    info!("Reloading tracing filter");
    if let Some(handle) = &console.config.filter_handle {
        if handle.reload(&data).is_err() {
            return Response::builder().status(404).body("".to_owned()).unwrap();
        }
        return Response::new(data);
    }
    Response::builder().status(404).body("".to_owned()).unwrap()
}
