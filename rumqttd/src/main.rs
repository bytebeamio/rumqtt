use rumqttd::{Broker, Config};

use structopt::StructOpt;

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
    config: String,
    /// log level (v: info, vv: debug, vvv: trace)
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,
}

fn main() {
    let commandline: CommandLine = CommandLine::from_args();

    banner(&commandline);
    let level = match commandline.verbose {
        0 => "rumqttd=warn",
        1 => "rumqttd=info",
        2 => "rumqttd=debug",
        _ => "rumqttd=trace",
    };

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

    let config = config::Config::builder()
        .add_source(config::File::with_name(&commandline.config))
        .build()
        .unwrap();

    let mut config: Config = config.try_deserialize().unwrap();
    config.console.set_filter_reload_handle(reload_handle);

    // println!("{:#?}", config);

    let mut broker = Broker::new(config);
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
