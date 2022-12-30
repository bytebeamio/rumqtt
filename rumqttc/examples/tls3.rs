//! Example of how to configure rumqttd to connect to a server using TLS and authentication.
#[cfg(feature = "use-native-tls")]
use rumqttc::TlsConnectorBuilder;
use std::error::Error;

#[cfg(feature = "use-native-tls")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use rumqttc::{self, AsyncClient, MqttOptions, TlsConfiguration, Transport};

    pretty_env_logger::init();
    color_backtrace::install();

    // let mut mqttoptions = MqttOptions::new("test-1", "broker-cn.emqx.io", 8883);
    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 8883);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));

    let builder = TlsConnectorBuilder::default();
    let transport = Transport::Tls(TlsConfiguration::NativeTls(builder));

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
