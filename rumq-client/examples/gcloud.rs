use futures_util::stream::StreamExt;
use std::thread;
use rumq_core::*;
use std::sync::Arc;
use futures_channel::mpsc;

use rumq_client::{self, MqttOptions, Request, connect, MqttEventLoop};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};
use jsonwebtoken::{encode, Algorithm, Header, Key};
use std::ops::Add;
// use futures_util::SinkExt;

// RUST_LOG=rumq_client=debug PROJECT=cloudlinc REGISTRY=iotcore cargo run --color=always --package rumq-client --example gcloud

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (mut requests_tx, requests_rx) = mpsc::channel(1);
    let mut eventloop = eventloop().await;
    let mut stream  = eventloop.build(requests_rx).await.unwrap();

    thread::spawn(move || {
        for i in 0..10 {
            let publish = publish(i);
            // futures_executor::block_on(requests_tx.send(publish)).unwrap();
            thread::sleep(Duration::from_secs(1));
        }

        thread::sleep(Duration::from_secs(100));

        for i in 0..10 {
            let publish = publish(i);
            // futures_executor::block_on(requests_tx.send(publish)).unwrap();
            thread::sleep(Duration::from_secs(1));
        }

        thread::sleep(Duration::from_secs(300));
    });

    while let Some(item) = stream.next().await {
        println!("{:?}", item);
    }
}

async fn eventloop<I>() -> MqttEventLoop<I> {
    let mqttoptions = MqttOptions::new(&id(), "mqtt.googleapis.com", 8883);
    let mqttoptions = mqttoptions.set_keep_alive(15);
    let password = gen_iotcore_password();
    let mqttoptions = mqttoptions
                                    .set_ca(include_bytes!("../certs/bike-1/roots.pem").to_vec())
                                    .set_credentials("unused", &password);


    let timeout = Duration::from_secs(10);
    connect(mqttoptions, timeout).await.unwrap()
}

fn publish(i: u8) -> Request {
    let topic = "/devices/".to_owned() +  "bike-1/events/imu";

    let publish = Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic_name: topic,
        pkid: None,
        payload: Arc::new(vec![1, 2, 3, i]),
    };

    Request::Publish(publish)
}

fn id() -> String {
    let project = env!("PROJECT").to_owned();
    let registry= env!("REGISTRY").to_owned();

    "projects/".to_owned() + &project + "/locations/asia-east1/registries/" + &registry + "/devices/" + "bike-1"
}

fn gen_iotcore_password() -> String {
    let key = include_bytes!("../certs/bike-1/rsa_private.der");
    let project = env!("PROJECT").to_owned();

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iat: u64,
        exp: u64,
        aud: String,
    }

    let jwt_header = Header::new(Algorithm::RS256);
    let iat = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let exp = SystemTime::now().add(Duration::from_secs(300)).duration_since(UNIX_EPOCH).unwrap().as_secs();

    let claims = Claims { iat, exp, aud: project };
    let key = Key::Der(key);
    encode(&jwt_header, &claims, key).unwrap()
}