use flume::bounded;
use rumqttc::{Client, MqttOptions, PromiseError, QoS};
use std::error::Error;
use std::thread::{self, sleep};
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
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
    for (i, qos) in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce]
        .into_iter()
        .enumerate()
    {
        match client
            .publish("hello/world", qos, false, vec![1; i])
            .unwrap()
            .blocking_wait()
        {
            Ok(pkid) => println!("Acknowledged Pub({pkid})"),
            Err(e) => println!("Publish failed: {e:?}"),
        }
    }

    // Spawn threads for each publish, use channel to notify result
    let (tx, rx) = bounded(1);

    for (i, qos) in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce]
        .into_iter()
        .enumerate()
    {
        let token = client
            .publish("hello/world", qos, false, vec![1; i])
            .unwrap();
        let tx = tx.clone();
        thread::spawn(move || {
            let res = token.blocking_wait();
            tx.send(res).unwrap()
        });
    }

    // Try resolving a promise, if it is waiting to resolve, try again after a sleep of 1s
    let mut token = client
        .publish("hello/world", QoS::AtMostOnce, false, vec![1; 4])
        .unwrap();
    thread::spawn(move || loop {
        match token.try_resolve() {
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
