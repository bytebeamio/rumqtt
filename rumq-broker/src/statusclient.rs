use crate::router::RouterMessage;

use tokio::sync::mpsc::{channel, Sender};
use rumq_core::QoS;

use hyper::{Client, Request, Body};
use hyper::body::Bytes;

pub async fn start(mut router_tx: Sender<RouterMessage>) {
    let (this_tx, mut this_rx) = channel(100);
    let client = Client::new();

    // construct connect router message with client id and handle to this connection 
    let routermessage = RouterMessage::Connect(("statusclient".to_owned(), this_tx));
    router_tx.send(routermessage).await.unwrap();

    let mut subscription = rumq_core::empty_subscribe();
    subscription.add("device/status".to_owned(), QoS::AtLeastOnce);
    let routermessage = RouterMessage::Subscribe(("statusclient".to_owned(), subscription));
    router_tx.send(routermessage).await.unwrap();

    loop {
        let publish = match this_rx.recv().await.unwrap() {
            RouterMessage::Publish(p) => p,
            _ => {
                error!("Invalid message. Expecting only status publishes");
                continue;
            }
        };

        info!("Status update...");

        let payload = publish.payload();
        // let body = Bytes::from(payload);
        let body = Bytes::from(&b"hello world"[..]);
        let request = Request::post("http://localhost:8000/").body(body.into()).unwrap();
        client.request(request).await.unwrap();
    }
}
