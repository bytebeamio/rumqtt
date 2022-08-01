//! Example of how to configure rumqttd to connect to a server using TLS and authentication.
use std::error::Error;

#[cfg(feature = "use-rustls")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use rumqttc::{self, AsyncClient, Key, MqttOptions, TlsConfiguration, Transport};

    pretty_env_logger::init();
    color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 8883);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));

    // Dummies to prevent compilation error in CI
    let ca = vec![1, 2, 3];
    let client_cert = vec![1, 2, 3];
    let client_key = vec![1, 2, 3];
    //     let ca = include_bytes!("/home/tekjar/tlsfiles/ca.cert.pem");
    //     let client_cert = include_bytes!("/home/tekjar/tlsfiles/device-1.cert.pem");
    //     let client_key = include_bytes!("/home/tekjar/tlsfiles/device-1.key.pem");

    let transport = Transport::Tls(TlsConfiguration::Simple {
        ca,
        alpn: None,
        client_auth: Some((client_cert, Key::RSA(client_key))),
    });

    mqttoptions.set_transport(transport);

    let (_client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    loop {
        match eventloop.poll().await {
            Ok(v) => {
                println!("Event = {:?}", v);
            }
            Err(e) => {
                println!("Error = {:?}", e);
                break;
            }
        }
    }

    Ok(())
}

#[cfg(not(feature = "use-rustls"))]
fn main() -> Result<(), Box<dyn Error>> {
    panic!("Enable feature 'use-rustls'");
}
