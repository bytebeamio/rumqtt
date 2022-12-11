use config::FileFormat;
use rumqttd::Broker;

use structopt::StructOpt;

static RUMQTTD_DEFAULT_CONFIG: &str = include_str!("../demo.toml");

#[derive(StructOpt)]
#[structopt(name = "rumqttd")]
#[structopt(about = "A high performance, lightweight and embeddable MQTT broker written in Rust.")]
#[structopt(author = "tekjar <raviteja@bytebeam.io>")]
struct CommandLine {
    /// Binary version
    #[structopt(skip = env!("VERGEN_BUILD_SEMVER"))]
    version: String,
    /// Build profile
    #[structopt(skip= env!("VERGEN_CARGO_PROFILE"))]
    profile: String,
    /// Commit SHA
    #[structopt(skip= env!("VERGEN_GIT_SHA"))]
    commit_sha: String,
    /// Commit SHA
    #[structopt(skip= env!("VERGEN_GIT_COMMIT_TIMESTAMP"))]
    commit_date: String,
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

    banner(&commandline);
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

fn banner(commandline: &CommandLine) {
    if commandline.quiet {
        return;
    }

    const B: &str = r#"                                              
         ___ _   _ __  __  ___ _____ _____ ___  
        | _ \ | | |  \/  |/ _ \_   _|_   _|   \ 
        |   / |_| | |\/| | (_) || |   | | | |) |
        |_|_\\___/|_|  |_|\__\_\|_|   |_| |___/ 
    "#;

    println!("{}", B);
    println!("    version: {}", commandline.version);
    println!("    profile: {}", commandline.profile);
    println!("    commit_sha: {}", commandline.commit_sha);
    println!("    commit_date: {}", commandline.commit_date);
    println!("\n");
}
