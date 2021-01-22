use argh::FromArgs;
use librumqttd::{Broker, Config};
use std::path::PathBuf;

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
    /// enable profiling
    #[argh(switch, short = 'p')]
    profile: bool,
}

fn main() {
    pretty_env_logger::init();
    let commandline: CommandLine = argh::from_env();
    let config: Config = confy::load_path(&commandline.config).unwrap();

    #[cfg(not(target_env = "msvc"))]
    let guard = if commandline.profile {
        Some(pprof::ProfilerGuard::new(100).unwrap())
    } else {
        None
    };

    let o = Broker::new(config).start();
    println!("Stopping broker!! Error = {:?}", o);

    #[cfg(not(target_env = "msvc"))]
    if commandline.profile {
        profile("rumqttd.pb", &guard.unwrap());
    }
}

#[cfg(not(target_env = "msvc"))]
fn profile(name: &str, guard: &pprof::ProfilerGuard) {
    use pprof::protos::Message;
    use std::{fs::File, io::Write as _};

    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}
