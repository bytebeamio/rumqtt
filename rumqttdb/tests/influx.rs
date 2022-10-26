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
    const INFLUX_BASE_URL: &'static str = "http://localhost:8086";
    const TEST_DB: &'static str = "test_influx";

    pub fn new() -> DbConn {
        let req = ureq::agent();
        let url = Url::parse(Self::INFLUX_BASE_URL).unwrap();
        let mut url = url.join("query").unwrap();

        req.post(url.as_str())
            .send_form(&[("q", &format!("DROP DATABASE {};", Self::TEST_DB))])
            .unwrap();

        req.post(url.as_str())
            .send_form(&[("q", &format!("CREATE DATABASE {};", Self::TEST_DB))])
            .unwrap();

        url.query_pairs_mut().append_pair("db", Self::TEST_DB);

        DbConn {
            conn: req.post(url.as_str()),
        }
    }

    pub fn query(&mut self, q: &str) -> Result<Response, Error> {
        self.conn
            .clone()
            .send_form(&[("db", Self::TEST_DB), ("q", q)])
    }
}

fn insert_runner(table_name: &str, insert_data: Vec<Value>) {
    // create test table
    let mut db = DbConn::new();

    // insert data in test table via test app
    let conf = DatabaseConfig {
        db_type: Type::InfluxV1,
        db_name: DbConn::TEST_DB.to_string(),
        server: "http://localhost:8086".to_owned(),
        user: None,
        password: None,
        secure: false,
        flush_interval: 1,
        mapping: None,
    };
    let app = common::TestApp::from(conf);
    app.insert(table_name, insert_data.clone());
    app.finish();

    let res = db
        .query(format!("SELECT * FROM {}", table_name).as_str())
        .unwrap();

    let res_json: Value = serde_json::from_str(&res.into_string().unwrap()).unwrap();
    let res = &res_json["results"][0]["series"][0]["values"]
        .as_array()
        .unwrap()
        .len();

    assert_eq!(*res, insert_data.len());
}

#[test]
#[serial]
fn insert() {
    let table_name = "test";

    let insert_data = vec![
        json!({ "id": "A", "sequence": 1, "timestamp": 1546300800123i64, "v1": 123, "v2": -123 }),
        json!({ "id": "A-B", "sequence": 2, "timestamp": 1546300800124i64, "v1": 123, "v2": -123 }),
        json!({ "id": "A-B-C", "sequence": 3, "timestamp": 1546300800125i64, "v1": -123, "v2": 123.321 }),
    ];

    insert_runner(table_name, insert_data);
}
