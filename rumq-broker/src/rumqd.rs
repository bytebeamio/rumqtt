use tokio::task;
use futures_util::future::join_all;
use serde::Deserialize;
use structopt::StructOpt;

use std::fs;
use std::sync::Arc;
use std::path::PathBuf;

use librumqd::{accept_loop, ServerSettings};

#[derive(StructOpt, Debug)]
#[structopt(name = "Rumqd", about = "High performance asynchronous mqtt broker")]
pub struct CommandLine {
    #[structopt(short = "c", help = "Rumqd config file", default_value = "rumqd.conf", parse(from_os_str))]
    config_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct Config {
    servers: Vec<ServerSettings>
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let commandline = CommandLine::from_args();
    let config = fs::read_to_string(commandline.config_path).unwrap();
    let config = toml::from_str::<Config>(&config).unwrap();
   
    let mut servers = Vec::new();
    for server in config.servers.into_iter() {
        let config = Arc::new(server);

        let fut = accept_loop(config);
        let o = task::spawn(fut);
        servers.push(o);
    }

    join_all(servers).await;
}
