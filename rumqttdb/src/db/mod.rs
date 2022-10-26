use enum_dispatch::enum_dispatch;

mod clickhouse;
mod influx;
mod options;
mod timescale;

use crate::config::{Record, Type};
use crate::{DatabaseConfig, Error};

use clickhouse::Clickhouse;
use influx::{Influx, InfluxV1, InfluxV2};
use options::ConnectOptions;
use timescale::Timescale;

/// A trait for writing into a Database
#[enum_dispatch(Database)]
pub trait DatabaseWrite {
    /// Writes the `payload` to the `table`, returing whether write succeeded
    fn write(&mut self, table: &str, payload: &mut Vec<Record>) -> Result<(), Error>;

    /// Closes the connection the Database, returning whether close succeeded
    fn close(&mut self) -> Result<(), Error>;
}

/// Rumqtt supported Databases
#[enum_dispatch]
pub enum Database {
    Clickhouse(Clickhouse),
    Timescale(Timescale),
    Influx(Influx),
}

impl Database {
    /// Create a new connection to Database
    pub fn connect(config: &DatabaseConfig) -> Database {
        let options = ConnectOptions::with_config(config);

        match config.db_type {
            Type::Clickhouse => Self::Clickhouse(Clickhouse::connect(options)),
            Type::Timescale => Self::Timescale(Timescale::connect(options)),
            Type::InfluxV1 => Self::Influx(InfluxV1::connect(options)),
            Type::InfluxV2 => Self::Influx(InfluxV2::connect(options)),
        }
    }
}
