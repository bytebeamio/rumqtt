use argh::FromArgs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::thread;

use librumqttd::Broker;
use pprof::ProfilerGuard;
use prost::Message;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::process::exit;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct Config {
    broker: librumqttd::Config,
}

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

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();
    let config: Config = confy::load_path(commandline.config)?;

    let guard = pprof::ProfilerGuard::new(100).unwrap();
    ctrlc::set_handler(move || {
        profile("rumqttd.pb", &guard);
        exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    let thread = thread::Builder::new().name("rumqttd-main".to_owned());

    let thread = thread
        .spawn(move || Broker::new(config.broker).start())
        .unwrap();

    println!("{:?}", thread.join());
    Ok(())
}

fn profile(name: &str, guard: &ProfilerGuard) {
    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}
