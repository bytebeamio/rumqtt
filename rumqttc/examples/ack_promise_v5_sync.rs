use flume::bounded;
use rumqttc::v5::{mqttbytes::QoS, Client, MqttOptions};
use std::error::Error;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    // color_backtrace::install();

    let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut conn) = Client::new(mqttoptions, 10);
    thread::spawn(move || {
        for event in conn.iter() {
            match &event {
                Ok(v) => {
                    println!("Event = {v:?}");
                }
                Err(e) => {
                    println!("Error = {e:?}");
                }
            }
        }
    });

    // Subscribe and wait for broker acknowledgement
    let pkid = client
        .subscribe("hello/world", QoS::AtMostOnce)
        .unwrap()
        .blocking_recv()
        .unwrap();
    println!("Acknowledged Subscribe({pkid})");

    // Publish at all QoS levels and wait for broker acknowledgement
    let pkid = client
        .publish("hello/world", QoS::AtMostOnce, false, vec![1; 1])
        .unwrap()
        .blocking_recv()
        .unwrap();
    println!("Acknowledged Pub({pkid})");

    let pkid = client
        .publish("hello/world", QoS::AtLeastOnce, false, vec![1; 2])
        .unwrap()
        .blocking_recv()
        .unwrap();
    println!("Acknowledged Pub({pkid})");

    let pkid = client
        .publish("hello/world", QoS::ExactlyOnce, false, vec![1; 3])
        .unwrap()
        .blocking_recv()
        .unwrap();
    println!("Acknowledged Pub({pkid})");

    // Spawn threads for each publish, use channel to notify result
    let (tx, rx) = bounded(1);

    let future = client
        .publish("hello/world", QoS::AtMostOnce, false, vec![1; 1])
        .unwrap();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        let res = future.blocking_recv();
        tx_clone.send(res).unwrap()
    });

    let future = client
        .publish("hello/world", QoS::AtLeastOnce, false, vec![1; 2])
        .unwrap();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        let res = future.blocking_recv();
        tx_clone.send(res).unwrap()
    });

    let future = client
        .publish("hello/world", QoS::ExactlyOnce, false, vec![1; 3])
        .unwrap();
    thread::spawn(move || {
        let res = future.blocking_recv();
        tx.send(res).unwrap()
    });

    while let Ok(Ok(pkid)) = rx.recv() {
        println!("Acknowledged Pub({:?})", pkid);
    }

    Ok(())
}
