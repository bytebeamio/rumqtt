use rumqttdb::{DatabaseConfig, Type};

use serde_json::{json, Value};
use serial_test::serial;
use ureq::{Error, Request, Response};
use url::Url;

mod common;

struct DbConn {
    conn: Request,
}

impl DbConn {
    const CLICKHOUS_BASE_URL: &'static str = "http://localhost:8123";
    const TEST_DB: &'static str = "test_clickhouse";

    pub fn new() -> DbConn {
        let req = ureq::agent();
        let mut url = Url::parse(Self::CLICKHOUS_BASE_URL).unwrap();
        req.post(url.as_str())
            .send_bytes(format!("DROP DATABASE IF EXISTS {};", Self::TEST_DB).as_bytes())
            .unwrap();

        req.post(url.as_str())
            .send_bytes(format!("CREATE DATABASE {};", Self::TEST_DB).as_bytes())
            .unwrap();

        url.query_pairs_mut().append_pair("database", Self::TEST_DB);

        DbConn {
            conn: req.post(url.as_str()),
        }
    }

    pub fn query(&mut self, q: &str) -> Result<Response, Error> {
        self.conn.clone().send_bytes(q.as_bytes())
    }
}

fn insert_runner(table_name: &str, table_query: &str, insert_data: Vec<Value>) {
    // create test table
    let mut db = DbConn::new();
    db.query(&table_query).unwrap();

    // insert data in test table via test app
    let conf = DatabaseConfig {
        db_type: Type::Clickhouse,
        server: "http://localhost:8123".to_owned(),
        user: None,
        password: None,
        secure: false,
        flush_interval: 1,
    };
    let app = common::TestApp::from(conf);
    app.insert(DbConn::TEST_DB, table_name, insert_data.clone());
    app.finish();

    let res = db.query(&format!("SELECT * FROM {table_name};"));
    let len = res.unwrap().into_string().unwrap().lines().count();

    assert_eq!(len, insert_data.len());
}

#[test]
#[serial]
fn insert() {
    let table_name = "test";
    let table_query = format!(
        r#"
    CREATE TABLE {} 
    (	id String,
        sequence UInt32,
        timestamp DateTime64(3),
        v1 Int32,
        v2 Float64
    )
    ENGINE = Memory;
    "#,
        table_name
    );

    let insert_data = vec![
        json!({ "id": "A", "sequence": 1, "timestamp": 1546300800123i64, "v1": 123, "v2": -123 }),
        json!({ "id": "A", "sequence": 2, "timestamp": 1546300800123i64, "v1": -123, "v2": 123.321 }),
    ];

    insert_runner(table_name, &table_query, insert_data);
}
