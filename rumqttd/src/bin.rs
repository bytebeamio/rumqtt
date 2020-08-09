use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use argh::FromArgs;

use librumqttd::Broker;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use pprof::ProfilerGuard;
use prost::Message;
use std::process::exit;

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
    #[argh(option, short = 'c', default = "PathBuf::from(\"config/rumqttd.conf\")")]
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
    }).expect("Error setting Ctrl-C handler");

    // Start the broker
    let mut broker = Broker::new(config.broker);
    broker.start()?;
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
