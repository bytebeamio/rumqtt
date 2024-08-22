use rumqttc::v5::mqttbytes::v5::AuthProperties;
use rumqttc::v5::{mqttbytes::QoS, AsyncClient, MqttOptions};
use rumqttc::{TlsConfiguration, Transport};
use std::error::Error;
use std::sync::Arc;
use tokio::task;
use tokio_rustls::rustls::ClientConfig;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let pubsub_access_token = "";

    let mut mqttoptions = MqttOptions::new("client1-session1", "MQTT hostname", 8883);
    mqttoptions.set_authentication_method(Some("OAUTH2-JWT".to_string()));
    mqttoptions.set_authentication_data(Some(pubsub_access_token.into()));

    // Use rustls-native-certs to load root certificates from the operating system.
    let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
    root_cert_store.add_parsable_certificates(
        rustls_native_certs::load_native_certs().expect("could not load platform certs"),
    );

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    let transport = Transport::Tls(TlsConfiguration::Rustls(Arc::new(client_config.into())));

    mqttoptions.set_transport(transport);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    task::spawn(async move {
        client.subscribe("topic1", QoS::AtLeastOnce).await.unwrap();
        client
            .publish("topic1", QoS::AtLeastOnce, false, "hello world")
            .await
            .unwrap();

        // Re-authentication test.
        let props = AuthProperties {
            method: Some("OAUTH2-JWT".to_string()),
            data: Some(pubsub_access_token.into()),
            reason: None,
            user_properties: Vec::new(),
        };

        client.reauth(Some(props)).await.unwrap();
    });

    loop {
        let notification = eventloop.poll().await;

        match notification {
            Ok(event) => println!("{:?}", event),
            Err(e) => {
                println!("Error = {:?}", e);
                break;
            }
        }
    }

    Ok(())
}
