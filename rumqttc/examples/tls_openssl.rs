//! Example of how to configure rumqttc to connect to a server using OpenSSL TLS.
use std::error::Error;

use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Transport};

use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "test.mosquitto.org", 8886);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));

    // Build an OpenSSL connector with default CA roots
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    builder.set_verify(SslVerifyMode::PEER);
    builder.set_default_verify_paths()?;
    let connector = builder.build();

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
