use rumqttd::{Broker, Config, Notification};

use std::thread;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    pretty_env_logger::init();

    // As examples are compiled as seperate binary so this config is current path dependent. Run it
    // from root of this crate
    let config = config::Config::builder()
        .add_source(config::File::with_name("rumqttd.toml"))
        .build()
        .unwrap();

    let config: Config = config.try_deserialize().unwrap();

    dbg!(&config);

    let mut broker = Broker::new(config);
    let (_link_tx, mut link_rx) = broker.link("singlenode").unwrap();
    thread::spawn(move || {
        broker.start().unwrap();
    });

    let mut count = 0;
    loop {
        let notification = match link_rx.recv().unwrap() {
            Some(v) => v,
            None => continue,
        };

        match notification {
            Notification::Forward(forward) => {
                count += 1;
                println!(
                    "Topic = {:?}, Count = {}, Payload = {} bytes",
                    forward.publish.topic,
                    count,
                    forward.publish.payload.len()
                );
            }
            v => {
                println!("{v:?}");
            }
        }
    }
}
