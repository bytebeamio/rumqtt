use flume::{Receiver, RecvTimeoutError};
use log;

use crate::config::{Buffer, Record};
use crate::db::{Database, DatabaseWrite};
use crate::{DatabaseConfig, Error};
use std::{
    collections::HashMap,
    fmt::Debug,
    time::{Duration, Instant},
};

pub struct DatabaseConnector {
    database: Database,
    inserters: HashMap<String, Buffer>,
}

impl DatabaseConnector {
    /// Initialize connection to database. Receive `Record`s from channel and
    /// write them to database.
    ///
    /// Simple usage:
    ///
    /// ```no_run
    /// use rumqttdb::{DatabaseConfig, DatabaseConnector, Type};
    ///
    /// let config = DatabaseConfig::new(
    ///     crate::Type::Clickhouse,
    ///     "database_demo".into(),
    ///     "http://localhost:8123".into(),
    ///     5,
    /// );
    /// let (_, database_rx) = flume::bounded::<(String, Vec<Record>)>(10);
    /// std::thread::spawn(|| {
    ///     if let Err(e) = DatabaseConnector::start(config, database_rx) {
    ///         log::error!("Terminating database connector. Error: {e}");
    ///     };
    /// });
    /// ```
    pub fn start<T, R>(config: DatabaseConfig, rx: Receiver<(T, Vec<R>)>) -> Result<(), Error>
    where
        T: ToString,
        R: Into<Record> + Debug,
    {
        let mut db_connector = DatabaseConnector {
            database: Database::connect(&config),
            inserters: HashMap::new(),
        };

        // Receive records fro channel, extend internal buffer, flush on deadline, repeat
        let mut deadline = Instant::now() + Duration::from_secs(config.flush_interval);
        loop {
            let (stream, data) = match rx.recv_deadline(deadline) {
                Ok(v) => v,
                Err(RecvTimeoutError::Timeout) => {
                    db_connector.flush();
                    deadline = Instant::now() + Duration::from_secs(config.flush_interval);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            db_connector.insert(
                &stream.to_string(),
                data.into_iter().map(|d| d.into()).collect(),
            );
        }
    }

    /// Extend write buffer for the given stream
    fn insert(&mut self, stream: &String, mut data: Vec<Record>) {
        let start = Instant::now();
        let count = data.len();
        let stream = stream.to_string();

        let inserter = match self.inserters.get_mut(&stream) {
            Some(i) => i,
            None => {
                self.inserters
                    .insert(stream.to_owned(), Buffer::new(Vec::new()));
                self.inserters.get_mut(&stream).unwrap()
            }
        };

        inserter.as_mut().append(&mut data);
        log::debug!(
            "{:15.15}[O] {:20} payload = {}(objects) elapsed = {}(micros)",
            "fill",
            stream,
            count,
            start.elapsed().as_micros()
        );
    }

    /// Write to database for all streams
    fn flush(&mut self) {
        for (stream, inserter) in self.inserters.iter_mut() {
            let count = inserter.as_mut().len();
            if count == 0 {
                continue;
            }

            let start = Instant::now();
            match self.database.write(stream, inserter.as_mut()) {
                Ok(..) => {
                    let elapsed_micros = start.elapsed().as_micros();
                    log::info!(
                        "{:15.15}[O] {:20} stream = {} payload = {}(objects) elapsed = {}(micros) throughput = {}(object/s)", 
                        "",
                        "flush",
                        stream,
                        count,
                        elapsed_micros,
                        count as u128 * 1000 * 1000 / elapsed_micros
                    );
                }
                Err(err) => {
                    log::error!(
                        "{:15.15}[E] {:20} stream = {} error = {:?}",
                        "",
                        "flush-error",
                        stream,
                        err
                    );
                }
            }
            inserter.as_mut().clear();
        }
    }
}
