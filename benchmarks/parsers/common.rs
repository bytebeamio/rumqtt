use pprof::{protos::Message, ProfilerGuard};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;

#[allow(unused)]
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
pub(crate) struct Print {
    pub(crate) id: String,
    pub(crate) messages: usize,
    pub(crate) payload_size: usize,
    pub(crate) total_size_gb: f32,
    pub(crate) v4_write_throughput_gpbs: f32,
    pub(crate) v4_read_throughput_gpbs: f32,
    pub(crate) v5_write_throughput_gpbs: f32,
    pub(crate) v5_read_throughput_gpbs: f32,
}
