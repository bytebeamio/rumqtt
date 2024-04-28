
use rumqttc::v5::mqttbytes::{QoS, v5::AuthProperties};
use rumqttc::v5::{AsyncClient, MqttOptions, AuthManager};
use tokio::task;
use std::error::Error;
use std::sync::{Arc, Mutex};
use bytes::Bytes;
//use scram::ScramClient;
//use scram::client::ServerFirst;
use flume::bounded;

#[derive(Debug)]
struct ScramAuthManager <'a>{
    user: &'a str,
    password: &'a str,
    //scram: Option<ServerFirst<'a>>,
}

impl <'a> ScramAuthManager <'a>{
    fn new(user: &'a str, password: &'a str) -> ScramAuthManager <'a>{
        ScramAuthManager{
            user,
            password,
            //scram: None,
        }
    }

    fn auth_start(&mut self) -> Result<Option<Bytes>, String>{
        //let scram = ScramClient::new(self.user, self.password, None);
        //let (scram, client_first) = scram.client_first();
        //self.scram = Some(scram);

        //Ok(Some(client_first.into()))

        Ok(Some("client first message".into()))
    }
}

impl <'a> AuthManager for ScramAuthManager<'a> {
    fn auth_continue(&mut self, auth_method: Option<String>, auth_data: Option<Bytes>) -> Result<Option<Bytes>, String> {

        // Check if the authentication method is SCRAM-SHA-256
        //if auth_method.unwrap() != "SCRAM-SHA-256" {
        //    return Err("Invalid authentication method".to_string());
        //}

        //if self.scram.is_none() {
        //    return Err("Invalid state".to_string());
        //}

        //let scram = self.scram.take().unwrap();

        //let auth_data = String::from_utf8(auth_data.unwrap().to_vec()).unwrap();

        // Process the server first message and reassign the SCRAM state.
        //let scram = match(scram.handle_server_first(&auth_data)){
        //    Ok(scram) => scram,
        //    Err(e) => return Err(e.to_string()),
        //};

        // Get the client final message and reassign the SCRAM state.
        //let (_, client_final) = scram.client_final();

        //Ok(Some(client_final.into()))

        Ok(Some("client final message".into()))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {

    let mut authmanager = ScramAuthManager::new("user1", "123456");
    let client_first = authmanager.auth_start().unwrap();
    let authmanager = Arc::new(Mutex::new(authmanager));

    let mut mqttoptions = MqttOptions::new("auth_test", "127.0.0.1", 1883);
    mqttoptions.set_authentication_method(Some("SCRAM-SHA-256".to_string()));
    mqttoptions.set_authentication_data(client_first);
    mqttoptions.set_auth_manager(authmanager.clone());
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    
    let (tx, rx) = bounded(1);

    task::spawn(async move {
        client.subscribe("rumqtt_auth/topic", QoS::AtLeastOnce).await.unwrap();
        client.publish("rumqtt_auth/topic", QoS::AtLeastOnce, false, "hello world").await.unwrap();

        // Wait for the connection to be established.
        rx.recv_async().await.unwrap();

        // Reauthenticate using SCRAM-SHA-256
        let client_first = authmanager.clone().lock().unwrap().auth_start().unwrap();
        let properties = AuthProperties{
            authentication_method: Some("SCRAM-SHA-256".to_string()),
            authentication_data: client_first,
            reason_string: None,
            user_properties: Vec::new(),
        };
        client.reauth(Some(properties)).await.unwrap();
    });

    loop {
        let notification = eventloop.poll().await;

        match notification {
            Ok(event) => {
                println!("Event = {:?}", event);
                match(event){
                    rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::ConnAck(_)) => {
                        tx.send_async("Connected").await.unwrap();
                    }
                    _ => {},
                }
            }
            Err(e) => {
                println!("Error = {:?}", e);
                break;
            }
        }
    }
    
    Ok(())
}