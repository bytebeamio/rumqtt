use pprof::ProfilerGuard;
use prost::Message;
use std::fs::File;
use std::io::Write;

#[allow(unused)]
pub fn profile(name: &str, guard: ProfilerGuard) {
    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode( & mut content).unwrap();
        file.write_all( & content).unwrap();
    };
}