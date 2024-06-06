use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::{error::Error, time::Duration};
use tokio::{task, time};

#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
use rumqttc::Transport;
#[cfg(all(feature = "use-rustls", not(feature = "use-native-tls")))]
use tokio_rustls::rustls::ClientConfig;

#[cfg(feature = "use-native-tls")]
use tokio_native_tls::native_tls;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    // port parameter is ignored when scheme is websocket
    let mut mqttoptions = MqttOptions::new(
        "test-1",
        "wss://test.mosquitto.org:8081", 8081,
    );

    #[cfg(feature = "use-native-tls")]
    {
        // Use native-tls to load root certificates from the operating system.
        println!("Using native-tls to load root certificates from the operating system.");
        let mut builder = native_tls::TlsConnector::builder();
        let pem = vec![1, 2, 3];
        // let pem = include_bytes!("native-tls-cert.pem");
        let cert = native_tls::Certificate::from_pem(&pem)?;
        builder.add_root_certificate(cert);
        let connector = builder.build()?;
        mqttoptions.set_transport(Transport::wss_with_config(connector.into()));
    }
    #[cfg(all(feature = "use-rustls", not(feature = "use-native-tls")))]
    {
        // Use rustls-native-certs to load root certificates from the operating system.
        println!("Using rustls-native-certs to load root certificates from the operating system.");
        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
        root_cert_store.add_parsable_certificates(
            rustls_native_certs::load_native_certs().expect("could not load platform certs"),
        );

        let client_config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        mqttoptions.set_transport(Transport::wss_with_config(client_config.into()));
    }

    mqttoptions.set_keep_alive(Duration::from_secs(60));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    task::spawn(async move {
        requests(client).await;
        time::sleep(Duration::from_secs(3)).await;
    });

    loop {
        let event = eventloop.poll().await;
        match event {
            Ok(notif) => {
                println!("Event = {notif:?}");
            }
            Err(err) => {
                println!("Error = {err:?}");
                return Ok(());
            }
        }
    }
}

async fn requests(client: AsyncClient) {
    client
        .subscribe("hello/world", QoS::AtMostOnce)
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .publish("hello/world", QoS::ExactlyOnce, false, vec![1; i])
            .await
            .unwrap();

        time::sleep(Duration::from_secs(1)).await;
    }

    time::sleep(Duration::from_secs(120)).await;
}
