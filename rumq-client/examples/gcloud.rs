use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::ops::Add;
use std::env;
use std::fs;

use rumq_client::{self, MqttOptions, Request, MqttEventLoop, eventloop};
use serde::{Serialize, Deserialize};
use jsonwebtoken::{encode, Algorithm, Header};
use futures_util::stream::StreamExt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task;
use tokio::time;

// RUST_LOG=rumq_client=debug PROJECT=cloudlinc REGISTRY=iotcore cargo run --color=always --package rumq-client --example gcloud

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    color_backtrace::install();

    let (requests_tx, requests_rx) = channel(1);
    let mqttoptions = gcloud();
    let mut eventloop = eventloop(mqttoptions, requests_rx);

    thread::spawn(move || {
        requests(requests_tx);
        thread::sleep(Duration::from_secs(100));
    });

    stream_it(&mut eventloop).await;
    println!("State = {:?}", eventloop.state);
}

async fn stream_it(eventloop: &mut MqttEventLoop) {
    let mut stream = eventloop.stream();

    while let Some(item) = stream.next().await {
        println!("{:?}", item);
    }
}

#[tokio::main(basic_scheduler)]
async fn requests(mut requests_tx: Sender<Request>) {
    task::spawn(async move {
        for i in 0..10 {
            requests_tx.send(publish_request(i)).await.unwrap();
            time::delay_for(Duration::from_secs(1)).await; 
        }

        time::delay_for(Duration::from_secs(100)).await; 
    }).await.unwrap();
}

fn gcloud() -> MqttOptions {
    let mut mqttoptions = MqttOptions::new(&id(), "mqtt.googleapis.com", 8883);
    mqttoptions.set_keep_alive(15);
    let password = gen_iotcore_password();
    let ca = fs::read("certs/bike-1/roots.pem").unwrap();
    
    mqttoptions
        .set_ca(ca)
        .set_credentials("unused", &password);

    mqttoptions 
}

fn publish_request(i: u8) -> Request {
    let topic = "/devices/".to_owned() +  "bike-1/events/imu";
    let payload = vec![1, 2, 3, i];

    let publish = rumq_client::publish(topic, payload);
    Request::Publish(publish)
}

fn id() -> String {
    let project = env::var("PROJECT").unwrap();
    let registry = env::var("REGISTRY").unwrap();

    "projects/".to_owned() + &project + "/locations/asia-east1/registries/" + &registry + "/devices/" + "bike-1"
}

fn gen_iotcore_password() -> String {
    let key = fs::read("certs/bike-1/rsa_private.pem").unwrap();
    let project = env::var("PROJECT").unwrap();
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
    encode(&jwt_header, &claims, &key).unwrap()
}
