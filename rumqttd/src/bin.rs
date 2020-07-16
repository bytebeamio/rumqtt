use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use argh::FromArgs;

use librumqttd::Broker;
use std::error::Error;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
struct Config {
    broker: librumqttd::Config,
}

#[derive(FromArgs, Debug)]
/// Command line args for rumqttd
struct CommandLine {
    /// path to config file
    #[argh(option, short = 'c', default = "PathBuf::from(\"rumqttd.conf\")")]
    config: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();
    let config: Config = confy::load_path(commandline.config)?;

    // Start the broker
    let mut broker = Broker::new(config.broker);
    broker.start()?;
    Ok(())
}