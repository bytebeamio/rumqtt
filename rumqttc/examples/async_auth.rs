
use rumqttc::v5::mqttbytes::{QoS, v5::AuthReasonCode, v5::AuthProperties, v5::Auth};
use rumqttc::v5::{AsyncClient, MqttOptions, AuthManagerTrait, StateError};
use tokio::task;
use std::error::Error;
use std::rc::Rc;
use std::cell::RefCell;
use scram::ScramClient;
use scram::client::ServerFirst;

#[derive(Debug)]
struct AuthManager <'a>{
    scram_client: Option<ScramClient<'a>>,
    scram_server: Option<ServerFirst<'a>>,
}

impl <'a> AuthManager <'a>{
    fn new(user: &'a str, password: &'a str) -> AuthManager <'a>{
        let scram = ScramClient::new(user, password, None);

        AuthManager{
            scram_client: Some(scram),
            scram_server: None,
        }
    }

    fn auth_start(&mut self) -> Result<String, StateError>{
        let scram = self.scram_client.take().unwrap();
        let (scram, client_first) = scram.client_first();
        self.scram_server = Some(scram);

        Ok(client_first)
    }
}

impl <'a> AuthManagerTrait for AuthManager<'a> {
    fn auth_continue(&mut self, auth_data: String) -> Result<String, String> {
        let scram = self.scram_server.take().unwrap();
        let scram = scram.handle_server_first(&auth_data).unwrap();
        let (_, client_final) = scram.client_final();
        Ok(client_final)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {

    let mut authmanager = AuthManager::new("user1", "123456");
    let client_first = authmanager.auth_start().unwrap();

    let mut mqttoptions = MqttOptions::new("auth_test", "127.0.0.1", 1883);
    mqttoptions.set_authentication_method(Some("SCRAM-SHA-256".to_string()));
    mqttoptions.set_authentication_data(Some(client_first.clone().into()));
    mqttoptions.set_auth_manager(Rc::new(RefCell::new(authmanager)));
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    task::spawn(async move {
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