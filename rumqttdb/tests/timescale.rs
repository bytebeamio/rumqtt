use once_cell::sync::Lazy;
use serde_json::{json, Value};
use serial_test::serial;
use sqlx::postgres::PgRow;
use sqlx::{Connection, Executor, PgConnection, PgPool};

use rumqttdb::DatabaseConfig;

use rumqttdb::*;

mod common;

const RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    return tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
});

struct DbConn {
    conn: sqlx::PgPool,
}

impl DbConn {
    const PG_BASE_URL: &'static str = "postgres://postgres:password@localhost";
    const TEST_DB: &'static str = "test_timescale";

    pub fn new() -> DbConn {
        let conn = RUNTIME.block_on(async {
            // clean
            let mut conn = PgConnection::connect(Self::PG_BASE_URL).await.unwrap();
            conn.execute(format!("DROP DATABASE IF EXISTS {};", Self::TEST_DB).as_str())
                .await
                .unwrap();

            conn.execute(format!("CREATE DATABASE {};", Self::TEST_DB).as_str())
                .await
                .expect("Failed to create database.");

            // connect to test database
            let url = format!("{}/{}", Self::PG_BASE_URL, Self::TEST_DB);
            let conn = PgPool::connect(&url).await.unwrap();
            conn
        });

        DbConn { conn }
    }

    pub fn query(&mut self, q: &str) -> Result<Vec<PgRow>, sqlx::Error> {
        RUNTIME.block_on(self.conn.fetch_all(q))
    }
}

fn insert_runner(table_name: &str, table_query: &str, insert_data: Vec<Value>) {
    // create test table
    let mut db = DbConn::new();
    db.query(&table_query).unwrap();

    // insert data in test table via test app
    let conf = DatabaseConfig {
        db_type: Type::Timescale,
        server: "postgres://localhost".to_owned(),
        user: Some("postgres".to_string()),
        password: Some("password".to_string()),
        secure: false,
        flush_interval: 1,
    };
    let app = common::TestApp::from(conf);
    app.insert(DbConn::TEST_DB, table_name, insert_data.clone());
    app.finish();

    let res = db
        .query(&format!("SELECT * FROM {table_name};"))
        .unwrap()
        .len();
    assert_eq!(res, insert_data.len());
}

#[test]
#[serial]
fn insert() {
    let table_name = "test";
    let table_query = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {}
        (
            id text,
            sequence int,
            "timestamp" timestamp,
            v1 int,
            v2 decimal
        );
    "#,
        table_name
    );

    let insert_data = vec![
        json!({ "id": "A", "sequence": 1, "timestamp": 1546300800123i64, "v1": 123, "v2": -123 }),
        json!({ "id": "A", "sequence": 2, "timestamp": 1546300800123i64, "v1": -123, "v2": 123.321 }),
    ];

    insert_runner(table_name, &table_query, insert_data);
}
