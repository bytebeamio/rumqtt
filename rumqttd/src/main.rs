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

    let config: rumqttd::Config = config.try_deserialize().unwrap();

    println!("{:#?}", config);

    let mut broker = Broker::new(config.clone());

    #[cfg(feature = "rumqttdb")]
    {
        let db_processor_link = broker.link("rumqttdb").unwrap();
        std::thread::spawn(|| {
            rumqttdb_processor::run(db_processor_link, config);
        });
    }

    broker.start().unwrap();
}

#[cfg(feature = "rumqttdb")]
mod rumqttdb_processor {
    use flume::Sender;
    use log::{error, info};
    use rumqttd::{LinkError, LinkRx, LinkTx};
    use rumqttdb::DatabaseConnector;
    use serde_json::Value;
    use std::time::{Duration, Instant};

    pub fn run(db_processor_link: (LinkTx, LinkRx), config: rumqttd::Config) {
        if config.database.is_none() {
            info!("Database configuration not found. Skipping rumqttdb initialization.");
            return;
        }

        let (database_tx, database_rx) = flume::bounded(10);
        let database_config = config.database.clone().unwrap();
        std::thread::spawn(move || {
            DatabaseConnector::start(database_config, database_rx).unwrap();
        });

        process(db_processor_link, database_tx, config);
    }

    fn process(
        processor_broker_link: (LinkTx, LinkRx),
        mut database_tx: Sender<(String, Vec<Value>)>,
        config: rumqttd::Config,
    ) {
        // subscribe to a filter
        let (mut broker_tx, mut broker_rx) = processor_broker_link;

        let mapping = config.database.as_ref().unwrap().mapping.as_ref().unwrap();
        mapping.keys().for_each(|key| {
            broker_tx.subscribe(key).unwrap();
        });

        // receive notifications from broker
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match broker_rx.recv_deadline(deadline) {
                Ok(notification) => {
                    let notification = match notification {
                        Some(notification) => notification,
                        None => continue,
                    };

                    match notification {
                        rumqttd::Notification::Forward(forward) => {
                            let topic = std::str::from_utf8(&forward.publish.topic[..]).unwrap();
                            let tenant_stream =
                                mapping.get(topic).expect("Publish on unknown topic");

                            if let Err(e) = flush(
                                &mut database_tx,
                                &tenant_stream,
                                forward.publish.payload.into(),
                            ) {
                                error!("Failed to process data = {:?}", e);
                                continue;
                            }
                        }
                        rumqttd::Notification::Unschedule => broker_rx.ready().unwrap(),
                        _ => (),
                    }
                }
                Err(LinkError::RecvTimeout(_)) => continue,
                Err(e) => {
                    error!("Data link receive error = {:?}", e);
                    break;
                }
            }
        }
    }

    fn flush(
        database_tx: &mut Sender<(String, Vec<Value>)>,
        tenant_stream: &str,
        payload: Vec<u8>,
    ) -> Result<(), ()> {
        let data: Vec<Value> = serde_json::from_slice(payload.as_ref()).unwrap();
        let mut output = Vec::new();
        for d in data {
            let d = match d {
                Value::Object(d) => d,
                _ => {
                    log::error!("Expecting an object on {} stream", tenant_stream);
                    continue;
                }
            };

            output.push(Value::Object(d));
        }

        info!(
            "Flushing: [tenant_stream] {} [len] {}",
            tenant_stream,
            output.len()
        );

        if output.len() > 0 {
            let data = (tenant_stream.to_owned(), output.to_owned());
            if let Err(e) = database_tx.send(data) {
                error!("Error sending data to rumqttdb = {:?}", e);
            };
        }

        Ok(())
    }
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
