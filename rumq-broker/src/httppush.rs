use crate::router::RouterMessage;
use crate::Config;

use rumq_core::{Packet, QoS};
use tokio::sync::mpsc::{channel, Sender};

use hyper::body::Bytes;
use hyper::{body, Client, Request};

use std::mem;
use std::sync::Arc;

pub async fn start(config: Arc<Config>, mut router_tx: Sender<(String, RouterMessage)>) {
    let (this_tx, mut this_rx) = channel(100);

    // let https = HttpsConnector::new();
    // let client = Client::builder().build::<_, hyper::Body>(https);
    let client = Client::new();

    // construct connect router message with client id and handle to this connection
    let connect = rumq_core::connect("pushclient");
    let routermessage = RouterMessage::Connect((connect, this_tx));
    router_tx.send(("pushclient".to_owned(), routermessage)).await.unwrap();

    let mut subscription = rumq_core::empty_subscribe();
    subscription.add(config.httppush.topic.clone(), QoS::AtLeastOnce);
    
    let packet = Packet::Subscribe(subscription);
    let routermessage = RouterMessage::Packet(packet);
    router_tx.send(("pushclient".to_owned(), routermessage)).await.unwrap();

    loop {
        let packet = match this_rx.recv().await.unwrap() {
            RouterMessage::Packet(p) => p,
            _ => {
                error!("Invalid message. Expecting only status publishes");
                continue;
            }
        };

        let mut publish = match packet {
            Packet::Publish(p) => p,
            Packet::Suback(_s) => continue,
            _ => unimplemented!(),
        };

        let payload = mem::replace(&mut publish.payload, Arc::new(Vec::new()));
        let topic = mem::replace(&mut publish.topic_name, String::new());

        let url = config.httppush.url.clone() + &topic;
        let body = Bytes::from(Arc::try_unwrap(payload).unwrap());

        info!("Http push = {}", url);
        let request = match Request::post(url).header("Content-type", "application/json").body(body.into()) {
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
