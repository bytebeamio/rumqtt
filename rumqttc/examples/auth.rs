
use rumqttc::v5::mqttbytes::QoS;
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

    task::spawn(async move {
        let server_first: String = client.recv_server_auth_data().await.unwrap();
        let scram = scram.handle_server_first(&server_first).unwrap();
        let (scram, client_final) = scram.client_final();
        client.send_client_auth_data(client_final).await.unwrap();

        client.subscribe("rumqtt_auth/topic", QoS::AtLeastOnce).await.unwrap();
        client.publish("rumqtt_auth/topic", QoS::AtLeastOnce, false, "hello world").await.unwrap();
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