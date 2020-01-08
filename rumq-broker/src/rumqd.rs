use tokio::task;
use structopt::StructOpt;

use std::fs;
use std::sync::Arc;
use std::path::PathBuf;

use librumqd::{accept_loop, Config};

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
    let config = toml::from_str::<Config>(&config).unwrap();
    let config = Arc::new(config);
    
    let fut = accept_loop(config, "0.0.0.0:1883");
    let o = task::spawn(fut).await;

    println!("{:?}", o);
}
