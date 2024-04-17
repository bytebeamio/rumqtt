
use rumqttc::v5::mqttbytes::{QoS, v5::AuthReasonCode, v5::AuthProperties, v5::Auth};
use rumqttc::v5::{AsyncClient, MqttOptions};
use tokio::task;
use std::error::Error;
use std::thread;
use scram::ScramClient;

#[tokio::main()]
async fn main() -> Result<(), Box<dyn Error>> {

    let scram = ScramClient::new("user1", "123456", None);
    let (scram, client_first) = scram.client_first();

    let mut mqttoptions = MqttOptions::new("auth_test", "127.0.0.1", 1883);
    mqttoptions.set_authentication_method(Some("SCRAM-SHA-256".to_string()));
    mqttoptions.set_authentication_data(Some(client_first.clone().into()));
    mqttoptions.set_connection_timeout(20);
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    let scram2 = ScramClient::new("user1", "123456", None);
    let (scram2, client_first2) = scram2.client_first();

    task::spawn(async move {
        let server_first: String = client.recv_server_auth_data().await.unwrap();
        let scram = scram.handle_server_first(&server_first).unwrap();
        let (scram, client_final) = scram.client_final();
        client.send_client_auth_data(client_final).await.unwrap();

        client.subscribe("rumqtt_auth/topic", QoS::AtLeastOnce).await.unwrap();
        client.publish("rumqtt_auth/topic", QoS::AtLeastOnce, false, "hello world").await.unwrap();

        // Reauthentication
        let props = AuthProperties {
            authentication_method: Some("SCRAM-SHA-256".to_string()),
            authentication_data: Some(client_first2.clone().into()),
            reason_string: None,
            user_properties: vec![],
        };

        client.auth(AuthReasonCode::Reauthenticate, Some(props)).await.unwrap();

        let server_first: String = client.recv_server_auth_data().await.unwrap();
        let scram2 = scram2.handle_server_first(&server_first).unwrap();
        let (scram2, client_final2) = scram2.client_final();
        client.send_client_auth_data(client_final2).await.unwrap();
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