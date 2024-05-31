//! Example of how to configure rumqttc to connect to a server using TLS and authentication.
use std::error::Error;

use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Transport};
use tokio_native_tls::native_tls;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "test.mosquitto.org", 8883);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));

    // Use native-tls to load root certificates from the operating system.
    let mut builder = native_tls::TlsConnector::builder();
    let pem = vec![1, 2, 3];
    // let pem = include_bytes!("native-tls-cert.pem");
    let cert = native_tls::Certificate::from_pem(&pem)?;
    builder.add_root_certificate(cert);
    let connector = builder.build()?;

    mqttoptions.set_transport(Transport::tls_with_config(connector.into()));

    let (_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                println!("Topic: {}, Payload: {:?}", p.topic, p.payload);
            }
            Ok(Event::Incoming(i)) => {
                println!("Incoming = {i:?}");
            }
            Ok(Event::Outgoing(o)) => println!("Outgoing = {o:?}"),
            Err(e) => {
                println!("Error = {e:?}");
                return Ok(());
            }
        }
    }
}
