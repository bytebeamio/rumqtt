use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::runtime::{self, Runtime};
use url::Url;

use crate::{config::Record, db::ConnectOptions, db::DatabaseWrite, Error};

pub struct Timescale {
    runtime: Runtime,
    conn: PgPool,
    query_builder: QueryBuilder<'static, Postgres>,
}

impl Timescale {
    pub fn connect(options: ConnectOptions) -> Timescale {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let mut url = Url::parse(&options.url).unwrap();
        url.query_pairs_mut()
            .append_pair("dbname", &options.database);

        if let Some(user) = &options.user {
            url.query_pairs_mut().append_pair("user", user);
        }

        if let Some(password) = &options.password {
            url.query_pairs_mut().append_pair("password", password);
        }

        if options.secure == true {
            url.query_pairs_mut().append_pair("sslmode", "allow");
        } else {
            url.query_pairs_mut().append_pair("sslmode", "disable");
        }

        let conn = runtime
            .block_on(PgPool::connect(url.as_str()))
            .expect("unable to connect to postgres");

        let query_builder: QueryBuilder<Postgres> = QueryBuilder::new("");

        Timescale {
            runtime,
            conn,
            query_builder,
        }
    }

    fn build_query(&mut self, table: &str, buffer: &mut Vec<Record>) -> Result<(), Error> {
        self.query_builder.push(format!("INSERT INTO {}(", table));

        // extract column names from first json object
        let first_obj = buffer.first().unwrap();
        let keys: Vec<String> = first_obj.keys();

        // add column names in query
        let mut separated_query = self.query_builder.separated(",");
        for k in keys.iter() {
            separated_query.push(k);
        }
        separated_query.push_unseparated(")");

        // add record values in query
        self.query_builder
            .push_values(buffer.clone(), |mut b, mut obj| {
                b.push_bind(obj.id);
                b.push_bind(obj.sequence);
                b.push_bind(obj.timestamp);

                for k in obj.other.as_object_mut().unwrap().values_mut() {
                    let val = k.take();
                    match val {
                        Value::Null => {
                            b.push_bind(Option::<bool>::None);
                        }
                        Value::Bool(v) => {
                            b.push_bind(v);
                        }
                        Value::Number(v) => {
                            if v.is_i64() {
                                b.push_bind(v.as_i64());
                            } else if v.is_f64() || v.is_u64() {
                                b.push_bind(v.as_f64());
                            }
                        }
                        Value::String(v) => {
                            b.push_bind(v);
                        }
                        Value::Array(v) => {
                            b.push_bind(v);
                        }
                        Value::Object(v) => {
                            b.push_bind(Value::Object(v));
                        }
                    };
                }
            });

        Ok(())
    }

    fn execute_query(&mut self) -> Result<(), Error> {
        let query = self.query_builder.build();
        self.runtime.block_on(query.execute(&self.conn)).unwrap();

        Ok(())
    }
}

impl DatabaseWrite for Timescale {
    fn write(&mut self, table: &str, payload: &mut Vec<Record>) -> Result<(), Error> {
        self.build_query(table, payload)?;
        self.execute_query()?;
        self.query_builder.reset();
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn parse_pg_url_correctly() {
    let url = "postgres://localhost:5432";
    let db_name = "test_db";

    let mut url = Url::parse(url).unwrap();
    url.query_pairs_mut().append_pair("database", db_name);

    println!("{url}");
}
