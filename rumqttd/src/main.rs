use config::FileFormat;
use rumqttd::Broker;

use structopt::StructOpt;

static RUMQTTD_DEFAULT_CONFIG: &str = include_str!("../rumqttd.toml");

#[derive(StructOpt)]
#[structopt(name = "rumqttd")]
#[structopt(about = "A high performance, lightweight and embeddable MQTT broker written in Rust.")]
#[structopt(author = "tekjar <raviteja@bytebeam.io>")]
struct CommandLine {
    /// path to config file
    #[structopt(short, long)]
    config: Option<String>,
    #[structopt(subcommand)]
    command: Option<Command>,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,
    /// launch without printing banner
    #[structopt(short, long)]
    quiet: bool,
}

#[derive(StructOpt)]
enum Command {
    /// Write default configuration file to stdout
    GenerateConfig,
}

fn main() {
    let commandline: CommandLine = CommandLine::from_args();

    if let Some(Command::GenerateConfig) = commandline.command {
        println!("{RUMQTTD_DEFAULT_CONFIG}");
        return;
    }

    if !commandline.quiet {
        banner();
    }

    let level = match commandline.verbose {
        0 => "rumqttd=warn",
        1 => "rumqttd=info",
        2 => "rumqttd=debug",
        _ => "rumqttd=trace",
    };

    // tracing syntax ->
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(level)
        .with_filter_reloading();

    let reload_handle = builder.reload_handle();

    builder
        .try_init()
        .expect("initialized subscriber succesfully");

    let mut config_builder = config::Config::builder();

    config_builder = match &commandline.config {
        Some(config) => config_builder.add_source(config::File::with_name(config)),
        None => config_builder.add_source(config::File::from_str(
            RUMQTTD_DEFAULT_CONFIG,
            FileFormat::Toml,
        )),
    };

    let mut configs: rumqttd::Config = config_builder.build().unwrap().try_deserialize().unwrap();
    configs.console.set_filter_reload_handle(reload_handle);

    // println!("{:#?}", configs);

    let mut broker = Broker::new(configs);
    broker.start().unwrap();
}

fn banner() {
    const B: &str = r#"                                              
         ___ _   _ __  __  ___ _____ _____ ___  
        | _ \ | | |  \/  |/ _ \_   _|_   _|   \ 
        |   / |_| | |\/| | (_) || |   | | | |) |
        |_|_\\___/|_|  |_|\__\_\|_|   |_| |___/ 
    "#;

    println!("{}\n", B);
}
