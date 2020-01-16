use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server};
use tokio::sync::mpsc::Sender;

use crate::router::RouterMessage;

use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn start(router_tx: Sender<RouterMessage>) {
    let addr = ([0, 0, 0, 0], 8080).into();

    let router_tx = Arc::new(Mutex::new(router_tx));
    let server = Server::bind(&addr).serve(make_service_fn(move |_| {
        let router_tx = router_tx.clone();
        async move { 
            let server_function = service_fn( move |request| {
                info!("Request = {:?}", request);
                let publish = rumq_core::publish("device/action", "hello world");
                let router_tx = router_tx.clone();
                
                async move {
                    let mut router_tx = router_tx.lock().await;
                    router_tx.send(RouterMessage::Publish(publish)).await.unwrap();
                    Ok::<_, hyper::Error>(Response::new(Body::from("Forwarding action")))
                }
            });

            Ok::<_, hyper::Error>(server_function)}
    }));

    server.await.unwrap();
}
