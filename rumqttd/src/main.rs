use rumqttd::Broker;

use simplelog::{
    Color, ColorChoice, CombinedLogger, Level, LevelFilter, LevelPadding, TargetPadding,
    TermLogger, TerminalMode, ThreadPadding,
};
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
    initialize_logging(&commandline);

    let config = config::Config::builder()
        .add_source(config::File::with_name(&commandline.config))
        .build()
        .unwrap();

    let config = config.try_deserialize().unwrap();

    println!("{:#?}", config);

    let mut broker = Broker::new(config);
    broker.start().unwrap();
}

fn initialize_logging(commandline: &CommandLine) {
    let mut config = simplelog::ConfigBuilder::new();

    let level = match commandline.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    config
        .set_location_level(LevelFilter::Off)
        .set_target_level(LevelFilter::Error)
        .set_target_padding(TargetPadding::Right(25))
        .set_thread_level(LevelFilter::Error)
        .set_thread_padding(ThreadPadding::Right(2))
        .set_level_color(Level::Trace, Some(Color::Cyan))
        .set_level_padding(LevelPadding::Right);

    config.add_filter_allow_str("rumqttd");

    let loggers = TermLogger::new(
        level,
        config.build(),
        TerminalMode::Mixed,
        ColorChoice::Always,
    );
    CombinedLogger::init(vec![loggers]).unwrap();
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
