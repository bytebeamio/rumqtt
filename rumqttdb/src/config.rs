use chrono::serde::ts_milliseconds;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;

pub use crate::db::{Database, DatabaseWrite};
pub use crate::DatabaseConnector;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub db_type: Type,
    pub db_name: String,
    pub server: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub secure: bool,
    pub flush_interval: u64,
    // MQTT topic to database table mapping for writes
    pub mapping: Option<HashMap<String, String>>,
}

/// Keys for supported databases
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Type {
    #[serde(rename = "clickhouse")]
    Clickhouse,
    #[serde(rename = "timescale")]
    Timescale,
    #[serde(rename = "influx1.8")]
    InfluxV1,
    #[serde(rename = "influx2")]
    InfluxV2,
}

/// Wrapper for vector of records
pub struct Buffer(Vec<Record>);

impl Buffer {
    pub fn new(buffer: Vec<Record>) -> Self {
        Buffer(buffer)
    }
}

impl AsMut<Vec<Record>> for Buffer {
    fn as_mut(&mut self) -> &mut Vec<Record> {
        &mut self.0
    }
}

/// A type that can be written to database by `Database`.
/// NOTE: Every `Record` must have the fields id, sequence, and timestamp.
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
    /// Return keys of a flattened record
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

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use serde_json::json;

    use crate::config::Record;

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
