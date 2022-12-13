use std::io::Write;

use config::FileFormat;
use rumqttd::Broker;

use structopt::StructOpt;

pub static RUMQTTD_DEFAULT_CONFIG: &str = include_str!("../rumqttd.toml");

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
}

#[derive(StructOpt)]
enum Command {
    /// Write default configuration file to stdout
    GenerateConfig,
}

fn main() {
    let commandline: CommandLine = CommandLine::from_args();

    if let Some(Command::GenerateConfig) = commandline.command {
        std::io::stdout()
            .write_all(RUMQTTD_DEFAULT_CONFIG.as_bytes())
            .unwrap();
        std::process::exit(0);
    }

    banner(&commandline);
    let level = match commandline.verbose {
        0 => "rumqttd=warn",
        1 => "rumqttd=info",
        2 => "rumqttd=debug",
        _ => "rumqttd=trace",
    };

    // tracing syntac ->
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

    let mut configs: rumqttd::Config;
    if let Some(config) = &commandline.config {
        configs = config::Config::builder()
            .add_source(config::File::with_name(config))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
    } else {
        configs = config::Config::builder()
            .add_source(config::File::from_str(
                RUMQTTD_DEFAULT_CONFIG,
                FileFormat::Toml,
            ))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
    }

    configs.console.set_filter_reload_handle(reload_handle);

    // println!("{:#?}", config);

    let mut broker = Broker::new(configs);
    broker.start().unwrap();
}

fn banner(commandline: &CommandLine) {
    const B: &str = r#"
        ██████╗ ██╗   ██╗███╗   ███╗ ██████╗ ████████╗████████╗██████╗
        ██╔══██╗██║   ██║████╗ ████║██╔═══██╗╚══██╔══╝╚══██╔══╝██╔══██╗
        ██████╔╝██║   ██║██╔████╔██║██║   ██║   ██║      ██║   ██║  ██║
        ██╔══██╗██║   ██║██║╚██╔╝██║██║▄▄ ██║   ██║      ██║   ██║  ██║
        ██║  ██║╚██████╔╝██║ ╚═╝ ██║╚██████╔╝   ██║      ██║   ██████╔╝
        ╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝ ╚══▀▀═╝    ╚═╝      ╚═╝   ╚═════╝

    "#;
    println!("{}", B);
    println!("    version: {}", commandline.version);
    println!("    profile: {}", commandline.profile);
    println!("    commit_sha: {}", commandline.commit_sha);
    println!("    commit_date: {}", commandline.commit_date);
    println!("\n");
}
