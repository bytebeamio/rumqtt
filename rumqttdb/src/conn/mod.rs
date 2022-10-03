use chrono::serde::ts_milliseconds;
use log;
use serde_json::Value;

use crate::ClientOptions;
use flume::{Receiver, RecvTimeoutError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use std::fmt::Debug;
use std::time::{Duration, Instant};

pub use crate::{Database, Inserter, Type};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Rx error = {0}")]
    Recv(#[from] RecvTimeoutError),
    #[error("rumqttdb error = {0}")]
    Rumqttdb(#[from] crate::Error),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub db_type: Type,
    pub server: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub secure: bool,
    pub flush_interval: u64,
    // pub topic_mapping: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Record {
    pub id: String,
    pub sequence: i32,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: chrono::DateTime<chrono::Utc>,
    #[serde(flatten)]
    pub other: Value,
}

impl Record {
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = vec!["id".into(), "sequence".into(), "timestamp".into()];
        let mut other_keys: Vec<String> = self
            .other
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.clone())
            .collect();

        keys.append(&mut other_keys);
        keys
    }
}

impl From<Value> for Record {
    fn from(item: Value) -> Record {
        serde_json::from_value::<Record>(item).unwrap()
    }
}

pub struct DatabaseConnector<T, R>
where
    T: ToString,
    R: Into<Record> + Debug,
{
    config: DatabaseConfig,
    rx: Receiver<(T, Vec<R>)>,
    inserters: HashMap<String, Database>,
}

impl<T, R> DatabaseConnector<T, R>
where
    T: ToString,
    R: Into<Record> + Debug,
{
    pub fn new(config: DatabaseConfig, rx: Receiver<(T, Vec<R>)>) -> DatabaseConnector<T, R> {
        DatabaseConnector {
            config,
            rx,
            inserters: HashMap::new(),
        }
    }

    pub fn start(&mut self) -> Result<(), Error> {
        let mut deadline = Instant::now() + Duration::from_secs(self.config.flush_interval);
        loop {
            let (tenant_stream, data) = match self.rx.recv_deadline(deadline) {
                Ok(v) => v,
                Err(RecvTimeoutError::Timeout) => {
                    self.flush();
                    deadline = Instant::now() + Duration::from_secs(self.config.flush_interval);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            self.insert(&tenant_stream, data.into_iter().map(|d| d.into()).collect());
        }
    }

    fn insert(&mut self, tenant_stream: &T, data: Vec<Record>) {
        let start = Instant::now();
        let count = data.len();
        let tenant_stream = tenant_stream.to_string();

        let inserter = match self.inserters.get_mut(&tenant_stream) {
            Some(i) => i,
            None => {
                let tokens: Vec<&str> = tenant_stream.split(".").collect();
                let (tenant_id, stream) = (tokens[0], tokens[1]);
                let mut options = ClientOptions::new(&self.config.server, &tenant_id);
                if let Some(username) = &self.config.user {
                    options = options.with_user(username);
                }
                if let Some(password) = &self.config.password {
                    options = options.with_password(password);
                }

                if self.config.secure {
                    options = options.with_security(true);
                }

                let inserter = Database::new(&self.config.db_type, options, &stream);
                self.inserters.insert(tenant_stream.to_owned(), inserter);
                self.inserters.get_mut(&tenant_stream).unwrap()
            }
        };

        match inserter.write(data) {
            Ok(..) => {
                log::debug!(
                    "{:15.15}[O] {:20} payload = {}(objects) elapsed = {}(micros)",
                    "fill",
                    tenant_stream,
                    count,
                    start.elapsed().as_micros()
                );
            }
            Err(err) => {
                log::error!(
                    "{:15.15}>[E] {:20}, stream = {} error = {:?} elapsed = {}(micros)",
                    "",
                    "fill-err",
                    tenant_stream,
                    err,
                    start.elapsed().as_micros()
                );
            }
        }
    }

    fn flush(&mut self) {
        for (tenant_stream, inserter) in self.inserters.iter_mut() {
            let count = inserter.len();
            if count == 0 {
                continue;
            }

            let start = Instant::now();
            match inserter.end() {
                Ok(..) => {
                    let elapsed_micros = start.elapsed().as_micros();
                    log::info!(
                        "{:15.15}[O] {:20} stream = {} payload = {}(objects) elapsed = {}(micros) throughput = {}(object/s)", 
                        "",
                        "flush",
                        tenant_stream,
                        count,
                        elapsed_micros,
                        count as u128 * 1000 * 1000 / elapsed_micros
                    );
                }
                Err(err) => {
                    inserter.clear();
                    log::error!(
                        "{:15.15}[E] {:20} stream = {} error = {:?}",
                        "",
                        "flush-error",
                        tenant_stream,
                        err
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use serde_json::json;

    use crate::conn::Record;

    #[test]
    fn json_value_parse_basic_success() {
        // does not have
        let test_value = json!({
            "id": "hello",
            "sequence": 32,
            "timestamp": 1663338562974u64,
        });

        let v: Record = serde_json::from_str(&test_value.to_string()).unwrap();
        println!("{:?}", v);
    }

    #[test]
    fn json_value_parse_complex_success() {
        // does not have
        let test_value = json!({
            "id": "hello",
            "sequence": 32,
            "timestamp": 1663338562974u64,
            "val1": 42,
            "val2": "world",
            "val3": json!({"val4": 123}),
        });

        let v: Record = serde_json::from_str(&test_value.to_string()).unwrap();
        println!("{:?}", v);

        let data = indoc! {r#"
        {
          "id": "hello",
          "sequence": 32,
          "timestamp": 1663338562974,
          "val1": 42,
          "val2": "world",
          "val3": {
            "val4": 123
          }
        }"#};

        let output = serde_json::to_string_pretty(&v).unwrap();
        assert_eq!(data, output);
    }

    #[test]
    #[should_panic]
    fn json_value_parse_fail() {
        // does not have required fields
        let test_value = json!({
            "id": "hello",
            "val1": 42,
            "val2": "world",
        });

        let _: Record = serde_json::from_str(&test_value.to_string()).unwrap();
    }

    #[test]
    fn verify_keys_are_flattened() {
        let test_value = json!({
            "id": "hello",
            "sequence": 32,
            "timestamp": 1663338562974u64,
            "val1": 42,
            "val2": "world",
            "val3": json!({"val4": 123}),
        });

        let v: Record = serde_json::from_str(&test_value.to_string()).unwrap();
        assert_eq!(
            Vec::from(["id", "sequence", "timestamp", "val1", "val2", "val3"]),
            v.keys()
        )
    }

    #[test]
    fn test() {
        let a = "hello";
        let p: Vec<&str> = a.split('.').collect();
        println!("{:?}", p);
    }
}
