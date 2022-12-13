use pprof::{protos::Message, ProfilerGuard};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;

pub fn profile(name: &str, guard: ProfilerGuard) {
    if let Ok(report) = guard.report().build() {
        let mut file = File::create(name).unwrap();
        let profile = report.pprof().unwrap();

        let mut content = Vec::new();
        profile.encode(&mut content).unwrap();
        file.write_all(&content).unwrap();
    };
}

#[derive(Serialize, Deserialize)]
pub struct Print {
    pub id: String,
    pub messages: usize,
    pub payload_size: usize,
    pub throughput: usize,
}
