use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, Server};
use tokio::sync::mpsc::Sender;

use crate::router::RouterMessage;


pub async fn start(router_tx: Sender<RouterMessage>) {
    let addr = ([0, 0, 0, 0], 8000).into();


    let server = Server::bind(&addr).serve(make_service_fn(|_| async move { 
        // let router_tx = router_tx.clone();
        
        let server_function = service_fn( |request| async move {
            info!("Request = {:?}", request);
            
            let publish = rumq_core::publish("device/action", "reboot");
            let router_message = RouterMessage::Publish(publish);
            // router_tx.send(router_message).await.unwrap();
            let response = Response::new(Body::from("Forwarding action"));
            Ok::<_, hyper::Error>(response)
        });

        Ok::<_, hyper::Error>(server_function)
    }));


    server.await.unwrap();
}
