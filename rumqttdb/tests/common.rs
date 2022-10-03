use std::thread::{self, JoinHandle};

use flume::Sender;
use log::debug;
use rumqttdb::{DatabaseConfig, DatabaseConnector};
use serde_json::Value;

pub struct TestApp {
    handle: JoinHandle<()>,
    db_tx: Sender<(String, Vec<Value>)>,
}

impl TestApp {
    pub fn from(conf: DatabaseConfig) -> TestApp {
        let (db_tx, db_rx) = flume::bounded(10);

        let handle = thread::spawn(move || {
            let mut db = DatabaseConnector::new(conf, db_rx);
            if let Err(e) = db.start() {
                debug!("[Closing connection] {}", e);
            }
        });

        TestApp { handle, db_tx }
    }

    pub fn insert(&self, database_name: &str, table_name: &str, insert_data: Vec<Value>) {
        self.db_tx
            .send((
                format!("{database_name}.{table_name}").to_string(),
                insert_data.clone(),
            ))
            .unwrap();
    }

    pub fn finish(self) {
        thread::sleep(std::time::Duration::from_secs(2));
        drop(self.db_tx);
        self.handle.join().unwrap();
    }
}
