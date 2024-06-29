use rumqttd::{Broker, Config};
use tracing::Level;

use core::time::Duration;
use std::thread;

fn main() {
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_max_level(Level::DEBUG)
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false);

    builder
        .try_init()
        .expect("initialized subscriber succesfully");

    // As examples are compiled as seperate binary so this config is current path dependent. Run it
    // from root of this crate
    let config = config::Config::builder()
        .add_source(config::File::with_name("rumqttd.toml"))
        .build()
        .unwrap();

    let config: Config = config.try_deserialize().unwrap();

    dbg!(&config);

    let handler = Broker::new(config).start().unwrap();

    thread::sleep(Duration::from_secs(1));
    handler.shutdown();
    thread::sleep(Duration::from_secs(1));
}
