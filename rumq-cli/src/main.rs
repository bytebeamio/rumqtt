use rumq_broker::Config;
use structopt::StructOpt;

use std::fs;
use std::path::PathBuf;

#[derive(StructOpt, Debug)]
#[structopt(name = "Rumqd", about = "High performance asynchronous mqtt broker")]
pub struct CommandLine {
    #[structopt(short = "c", help = "Rumqd config file", default_value = "rumqd.conf", parse(from_os_str))]
    config_path: PathBuf,
}


#[tokio::main(core_threads = 1)]
async fn main() {
    pretty_env_logger::init();

    let commandline = CommandLine::from_args();
    let config = fs::read_to_string(commandline.config_path).unwrap();
    let config = toml::from_str::<Config>(&config).unwrap();

    let mut broker = rumq_broker::new(config);
    let o = broker.start().await;
    println!("Result = {:?}", o);
}
