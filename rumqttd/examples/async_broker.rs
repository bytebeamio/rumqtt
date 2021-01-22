/*
use librumqttd::{async_locallink::construct_broker, Config};
use std::thread;

fn main() {
    pretty_env_logger::init();
    let config: Config = confy::load_path("config/rumqttd.conf").unwrap();

    let (mut router, console, servers, builder) = construct_broker(config);

    thread::spawn(move || {
        router.start().unwrap();
    });
    thread::spawn(console);

    // connect to get a receiver
    // TODO: Connect with a function which return tx and rx to prevent
    // doing publishes before connecting
    // NOTE: Connection buffer should be atleast total number of possible
    // topics + 3 (request types). If inflight is full with more topics
    // in tracker, it's possible that router never responnds current
    // inflight requests. But other pending requests should still be able
    // to progress
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    rt.build().unwrap().block_on(async {
        let (mut tx, mut rx) = builder.connect("localclient", 200).await.unwrap();
        tx.subscribe(std::iter::once("#")).await.unwrap();

        // subscribe and publish in a separate thread
        let pub_task = tokio::spawn(async move {
            for _ in 0..10usize {
                for i in 0..200usize {
                    let topic = format!("hello/{}/world", i);
                    tx.publish(topic, false, vec![0; 1024]).await.unwrap();
                }
            }
        });

        let sub_task = tokio::spawn(async move {
            let mut count = 0;
            loop {
                let message = rx.recv().await.unwrap();
                // println!("T = {}, P = {:?}", message.topic, message.payload.len());
                count += message.payload.len();
                println!("{}", count);
            }
        });

        servers.await;
        pub_task.await.unwrap();
        sub_task.await.unwrap();
    });
}
 */
fn main() {}
