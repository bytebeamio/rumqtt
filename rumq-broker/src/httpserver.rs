use hyper::service::{make_service_fn, service_fn};
use hyper::{body, Body, Response, Server};
use tokio::sync::mpsc::Sender;
use rumq_core::Packet;

use crate::router::RouterMessage;
use crate::Config;

use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn start(config: Arc<Config>, router_tx: Sender<(String, RouterMessage)>) {
    let addr = ([0, 0, 0, 0], config.httpserver.port).into();

    let router_tx = Arc::new(Mutex::new(router_tx));
    let server = Server::bind(&addr).serve(make_service_fn(move |_| {
        let router_tx = router_tx.clone();
        async move {
            let server_function = service_fn(move |request| {
                let router_tx = router_tx.clone();

                async move {
                    info!("Request = {:?}", request);
                    let path = request.uri().path().to_owned();
                    let body_bytes = body::to_bytes(request.into_body()).await?;
                    info!("Path = {:?}", path);
                    info!("Body = {:?}", body_bytes);

                    let publish = rumq_core::publish(path, rumq_core::QoS::AtMostOnce, body_bytes.to_vec());
                    let packet = Packet::Publish(publish);
                    let mut router_tx = router_tx.lock().await;
                    router_tx.send(("httpserver".to_owned(), RouterMessage::Packet(packet))).await.unwrap();

                    Ok::<_, hyper::Error>(Response::new(Body::from("Forwarding action")))
                }
            });

            Ok::<_, hyper::Error>(server_function)
        }
    }));

    server.await.unwrap();
}
