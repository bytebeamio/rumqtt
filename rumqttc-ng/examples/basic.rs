use rumqttc_ng::{builder::std::Builder, client::blocking::AckSetting, EventLoopSettings, Notification, QoS, TransportSettings};


fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    let mut clients = Builder::new()
        .register_client(0, 10 * 1024)
        .register_client(1, 100 * 1024)
        .set_eventloop(EventLoopSettings {
            max_subscriptions: 10,
            max_subscription_log_size: 100 * 1024 * 1024,
            max_inflight_messages: 100,
        })
        // set transport
        .set_transport(TransportSettings::Tcp { 
            host: "localhost".to_string(), 
            port: 1883,
            security: None,
        })
        // spawns eventloop in background
        .start(); 

    // Client returns tokens for callers to block until broker acknowledges
    let client = clients.get_mut(0).unwrap();

    // Block on each message
    client.subscribe("hello/world", QoS::AtMostOnce, AckSetting::Auto)?.wait()?;
    client.publish("hello/world", "Hello, world!", QoS::AtMostOnce, false)?.wait()?;

    // Block on a batch of messages
    let mut tokens = vec![];
    for _ in 0..10 {
        let token = client.publish("hello/world", "Hello, world!", QoS::AtMostOnce, false)?;
        tokens.push(token);
    }

    // Trait implementation
    // tokens.wait();


    // Subscriptions
    client.subscribe("hello/world", QoS::AtMostOnce, AckSetting::Manual)?.wait()?;
    client.capture_alerts();

    for notification in client.next() {
        println!("{:?}", notification);
        match notification {
            Notification::Message(message) => {
                println!("{:?}", message);
            }
            Notification::ManualAckMessage(message, token) => {
                println!("{:?}", message);
                // token.ack();
            }
            event => {
                println!("{:?}", event);
            }
            _ => {}
        }

    }

    // Convert to async client
    // let client: nonblocking::Client = clients.into();
    Ok(())
}
