use crate::router::RouterMessage;
use crate::Config;

use rumq_core::QoS;
use tokio::sync::mpsc::{channel, Sender};

use hyper::body::Bytes;
use hyper::{body, Client, Request};
use hyper_tls::HttpsConnector;

use std::mem;
use std::sync::Arc;

pub async fn start(config: Arc<Config>, mut router_tx: Sender<RouterMessage>) {
    let (this_tx, mut this_rx) = channel(100);

    let https = HttpsConnector::new();
    // let client = Client::builder().build::<_, hyper::Body>(https);
    let client = Client::new();

    // construct connect router message with client id and handle to this connection
    let routermessage = RouterMessage::Connect(("pushclient".to_owned(), this_tx));
    router_tx.send(routermessage).await.unwrap();

    let mut subscription = rumq_core::empty_subscribe();
    subscription.add(config.httppush.topic.clone(), QoS::AtLeastOnce);
    let routermessage = RouterMessage::Subscribe(("pushclient".to_owned(), subscription));
    router_tx.send(routermessage).await.unwrap();

    loop {
        let mut publish = match this_rx.recv().await.unwrap() {
            RouterMessage::Publish(p) => p,
            _ => {
                error!("Invalid message. Expecting only status publishes");
                continue;
            }
        };

        let payload = mem::replace(&mut publish.payload, Arc::new(Vec::new()));
        let body = Bytes::from(Arc::try_unwrap(payload).unwrap());
        let request = match Request::put(&config.httppush.url)
            .header("Content-type", "application/json")
            .body(body.into())
        {
            Ok(request) => request,
            Err(e) => {
                error!("Post create error = {:?}", e);
                continue;
            }
        };

        let o = match client.request(request).await {
            Ok(res) => res,
            Err(e) => {
                error!("Http request error = {:?}", e);
                continue;
            }
        };

        info!("Response = {:?}", o);

        let body_bytes = body::to_bytes(o.into_body()).await.unwrap();
        info!("Body = {:?}", body_bytes);
    }
}
