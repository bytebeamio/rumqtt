use argh::FromArgs;
use std::path::PathBuf;
use std::thread;

use librumqttd::{Broker, Config};
use std::process::exit;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(FromArgs, Debug)]
/// Command line args for rumqttd
struct CommandLine {
    /// path to config file
    #[argh(
        option,
        short = 'c',
        default = "PathBuf::from(\"config/rumqttd.conf\")"
    )]
    config: PathBuf,
}

fn main() {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();
    let config: Config = confy::load_path(commandline.config).unwrap();

    ctrlc::set_handler(move || {
        exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    let thread = thread::Builder::new().name("rumqttd-main".to_owned());
    let thread = thread.spawn(move || Broker::new(config).start()).unwrap();

    println!("{:?}", thread.join());
}
