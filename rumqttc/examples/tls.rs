//! Example of how to configure rumqttd to connect to a server using TLS and authentication.

use rumqttc::{self, AsyncClient, Event, Incoming, MqttOptions, Transport};
use rustls::ClientConfig;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqtt_options = MqttOptions::new("test-1", "mqtt.example.server", 8883);
    mqtt_options.set_keep_alive(std::time::Duration::from_secs(5));
    mqtt_options.set_credentials("username", "password");

    // Use rustls-native-certs to load root certificates from the operating system.
    let mut root_cert_store = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
        root_cert_store
            .add(&rustls::Certificate(cert.0))
            .unwrap();
    }

    let client_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    mqtt_options.set_transport(Transport::tls_with_config(client_config.into()));

    let (_client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                println!("Topic: {}, Payload: {:?}", p.topic, p.payload)
            }
            Ok(Event::Incoming(i)) => {
                println!("Incoming = {:?}", i);
            }
            Ok(Event::Outgoing(o)) => println!("Outgoing = {:?}", o),
            Err(e) => {
                println!("Error = {:?}", e);
            }
        }
    }
}
