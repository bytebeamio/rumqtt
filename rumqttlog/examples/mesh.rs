use argh::FromArgs;
use bytes::Bytes;
use std::path::PathBuf;
use std::{fs, thread};
use tokio::time::Duration;
use rumqttlog::{Config, Router, RouterInMessage};
use rumqttlog::router::Data;

#[derive(FromArgs)]
/// Reach new heights.
struct CommandLine {
    /// id of the router
    #[argh(option, short = 'i', default = "0")]
    id: u8,
    /// timestone config directory
    #[argh(option, short = 'd', default = "PathBuf::from(\"examples/mesh.toml\")")]
    config: PathBuf,
    /// number of records to write
    #[argh(option, short = 'x', default = "100")]
    record_count: usize,
    /// size of each record
    #[argh(option, short = 'y', default = "10")]
    record_size: usize,
}

#[tokio::main(core_threads = 1)]
async fn main() {
    pretty_env_logger::init();

    // load config and commandline
    let commandline: CommandLine = argh::from_env();
    let mut config: Config = confy::load_path(commandline.config).unwrap();

    // update few options in config based on router id
    config.id = commandline.id;

    let id = format!("{}", config.id);
    config.dir = config.dir.join(&id);
    let _ = fs::remove_dir_all(&config.dir);

    let (router, router_tx) = Router::new(config);

    // spawn the router in a separate system thread
    thread::spawn(move || run_router(router));
    let topic = "test/distributed/storage";
    for i in 0..commandline.record_count {
        if commandline.id != 0 {
            break;
        }

        let payload = vec![i as u8 % 255; commandline.record_size];
        let payload = Bytes::from(payload);
        let message = RouterInMessage::Data(Data { topic: topic.to_owned(), pkid: 0, payload });
        router_tx.send((100, message)).await.unwrap();
    }

    tokio::time::delay_for(Duration::from_secs(60)).await;
}

#[tokio::main(core_threads = 1)]
async fn run_router(mut router: Router) {
    router.start().await
}
