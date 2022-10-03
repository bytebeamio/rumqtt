mod clickhouse;
mod influx;
mod timescale;

use clickhouse::Clickhouse;
use enum_dispatch::enum_dispatch;
use influx::Influx;
use serde::{Deserialize, Serialize};
use timescale::Timescale;

use crate::{conn::Record, ClientOptions, Error};

#[enum_dispatch(Database)]
pub trait Inserter {
    fn get_write_buffer(&mut self) -> &mut Vec<Record>;

    fn len(&mut self) -> usize {
        self.get_write_buffer().len()
    }

    fn write(&mut self, mut payload: Vec<Record>) -> Result<(), Error> {
        self.get_write_buffer().append(&mut payload);
        Ok(())
    }

    fn clear(&mut self) {
        self.get_write_buffer().clear();
    }

    fn end(&mut self) -> Result<(), Error>;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Type {
    #[serde(rename = "clickhouse")]
    Clickhouse,
    #[serde(rename = "timescale")]
    Timescale,
    #[serde(rename = "influx1.8")]
    Influx1,
    #[serde(rename = "influx2")]
    Influx2,
}

#[enum_dispatch]
pub enum Database {
    Clickhouse,
    Timescale,
    Influx,
}

impl Database {
    pub fn new(db_type: &Type, options: ClientOptions, table: &str) -> Database {
        match db_type {
            Type::Clickhouse => Self::Clickhouse(Clickhouse::new(options, table)),
            Type::Timescale => Self::Timescale(Timescale::new(options, table)),
            Type::Influx1 | Type::Influx2 => Self::Influx(Influx::new(options, table, db_type)),
        }
    }
}
