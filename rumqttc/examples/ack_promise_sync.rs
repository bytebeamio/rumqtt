use flume::bounded;
use rumqttc::{Client, MqttOptions, PromiseError, QoS};
use std::error::Error;
use std::thread::{self, sleep};
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
    match client
        .subscribe("hello/world", QoS::AtMostOnce)
        .unwrap()
        .blocking_wait()
    {
        Ok(pkid) => println!("Acknowledged Sub({pkid})"),
        Err(e) => println!("Subscription failed: {e:?}"),
    }

    // Publish at all QoS levels and wait for broker acknowledgement
    match client
        .publish("hello/world", QoS::AtMostOnce, false, vec![1; 1])
        .unwrap()
        .blocking_wait()
    {
        Ok(pkid) => println!("Acknowledged Pub({pkid})"),
        Err(e) => println!("Publish failed: {e:?}"),
    }

    match client
        .publish("hello/world", QoS::AtLeastOnce, false, vec![1; 2])
        .unwrap()
        .try_resolve()
    {
        Ok(pkid) => println!("Acknowledged Pub({pkid})"),
        Err(e) => println!("Publish failed: {e:?}"),
    }

    match client
        .publish("hello/world", QoS::ExactlyOnce, false, vec![1; 3])
        .unwrap()
        .blocking_wait()
    {
        Ok(pkid) => println!("Acknowledged Pub({pkid})"),
        Err(e) => println!("Publish failed: {e:?}"),
    }

    // Spawn threads for each publish, use channel to notify result
    let (tx, rx) = bounded(1);

    let future = client
        .publish("hello/world", QoS::AtMostOnce, false, vec![1; 1])
        .unwrap();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        let res = future.blocking_wait();
        tx_clone.send(res).unwrap()
    });

    let future = client
        .publish("hello/world", QoS::AtLeastOnce, false, vec![1; 2])
        .unwrap();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        let res = future.blocking_wait();
        tx_clone.send(res).unwrap()
    });

    let mut future = client
        .publish("hello/world", QoS::ExactlyOnce, false, vec![1; 3])
        .unwrap();
    thread::spawn(move || loop {
        match future.try_resolve() {
            Err(PromiseError::Waiting) => {
                println!("Promise yet to resolve, retrying");
                sleep(Duration::from_secs(1));
            }
            res => {
                tx.send(res).unwrap();
                break;
            }
        }
    });

    while let Ok(res) = rx.recv() {
        match res {
            Ok(pkid) => println!("Acknowledged Pub({pkid})"),
            Err(e) => println!("Publish failed: {e:?}"),
        }
    }

    // Unsubscribe and wait for broker acknowledgement
    match client.unsubscribe("hello/world").unwrap().blocking_wait() {
        Ok(pkid) => println!("Acknowledged Unsub({pkid})"),
        Err(e) => println!("Unsubscription failed: {e:?}"),
    }

    Ok(())
}
