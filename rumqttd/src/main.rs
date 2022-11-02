use rumqttd::Broker;

use tracing::metadata::LevelFilter;
// use tracing_subscriber::{self, fmt, prelude::*, EnvFilter};
use tracing_subscriber::{filter::filter_fn, fmt, prelude::*, EnvFilter, Registry};

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
    let _level = match commandline.verbose {
        0 => LevelFilter::WARN,
        1 => LevelFilter::INFO,
        2 => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    };

    /* FOR TREE VIEW 
    let layer = tracing_tree::HierarchicalLayer::default()
        .with_writer(std::io::stdout)
        .with_indent_lines(true)
        .with_indent_amount(2)
        .with_thread_names(false)
        .with_thread_ids(false)
        .with_verbose_exit(false)
        .with_verbose_entry(false)
        .with_targets(true);

    let subscriber = Registry::default().with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    */


    // let filter = filter_fn(|meta| meta.target().contains("rumqttd"));

    // with filter layers
    tracing_subscriber::registry()
        .with(fmt::layer().compact())
        // .with(filter)
        .with(EnvFilter::from_default_env())
        .init();


    let config = config::Config::builder()
        .add_source(config::File::with_name(&commandline.config))
        .build()
        .unwrap();

    let config = config.try_deserialize().unwrap();

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
