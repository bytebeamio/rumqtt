use argh::FromArgs;
use std::path::PathBuf;
use std::thread;

use librumqttd::{Broker, Config};
use std::process::exit;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(FromArgs, Debug)]
/// Command line args for rumqttd
struct CommandLine {
    /// path to config file
    #[argh(
        option,
        short = 'c',
        default = "PathBuf::from(\"config/rumqttd.conf\")"
    )]
    config: PathBuf,
}

fn main() {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();
    let config: Config = confy::load_path(commandline.config).unwrap();

    #[cfg(not(target_env = "msvc"))]
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    ctrlc::set_handler(move || {
        #[cfg(not(target_env = "msvc"))]
        profile("rumqttd.pb", &guard);

        exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    let thread = thread::Builder::new().name("rumqttd-main".to_owned());
    let thread = thread.spawn(move || Broker::new(config).start()).unwrap();

    println!("{:?}", thread.join());
}

#[cfg(not(target_env = "msvc"))]
fn profile(name: &str, guard: &pprof::ProfilerGuard) {
    use prost::Message;
    use std::{fs::File, io::Write as _};

    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}
