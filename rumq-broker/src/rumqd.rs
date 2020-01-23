use structopt::StructOpt;

use std::fs;
use std::path::PathBuf;

#[derive(StructOpt, Debug)]
#[structopt(name = "Rumqd", about = "High performance asynchronous mqtt broker")]
pub struct CommandLine {
    #[structopt(short = "c", help = "Rumqd config file", default_value = "rumqd.conf", parse(from_os_str))]
    config_path: PathBuf,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let commandline = CommandLine::from_args();
    let config = fs::read_to_string(commandline.config_path).unwrap();
    let config = toml::from_str::<librumqd::Config>(&config).unwrap();

    librumqd::start(config).await
}
