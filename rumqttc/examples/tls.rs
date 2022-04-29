//! Example of how to configure rumqttd to connect to a server using TLS and authentication.
use std::error::Error;

#[cfg(feature = "use-rustls")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use rumqttc::{self, AsyncClient, Event, Incoming, MqttOptions, Transport};
    use rustls::ClientConfig;

    pretty_env_logger::init();
    color_backtrace::install();

    // Use rustls-native-certs to load root certificates from the operating system.
    let mut root_cert_store = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
        root_cert_store.add(&rustls::Certificate(cert.0))?;
    }

    let client_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let mqttoptions = MqttOptions::builder()
        .broker_addr("mqtt.example.server")
        .port(8883)
        .client_id("test-1".parse().expect("invalid client id"))
        .keep_alive(std::time::Duration::from_secs(5))
        .credentials(("username".into(), "password".into()))
        .transport(Transport::tls_with_config(client_config.into()))
        .build();

    let (_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                println!("Topic: {}, Payload: {:?}", p.topic, p.payload);
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

#[cfg(not(feature = "use-rustls"))]
fn main() -> Result<(), Box<dyn Error>> {
    panic!("Enable feature 'use-rustls'");
}
