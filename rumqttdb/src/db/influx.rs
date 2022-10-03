use bytes::BytesMut;
use ureq::Request;
use url::Url;

use crate::{conn::Record, ClientOptions, Error, Inserter, Type};

const BUFFER_SIZE: usize = 128 * 1024;

#[derive(Clone)]
enum RequestClient {
    V1(Request),
    V2(Request),
}

impl RequestClient {
    pub fn new(options: ClientOptions, db_type: &Type) -> RequestClient {
        match db_type {
            Type::Influx1 => {
                let mut url = Url::parse(&options.url).unwrap().join("write").unwrap();
                url.query_pairs_mut().append_pair("db", &options.database);

                let request = ureq::builder().build().post(url.as_str());
                RequestClient::V1(request)
            }
            Type::Influx2 => {
                let mut url = Url::parse(&options.url)
                    .unwrap()
                    .join("api/v2/write")
                    .unwrap();

                url.query_pairs_mut()
                    .append_pair("bucket", &options.database);

                url.query_pairs_mut()
                    .append_pair("org", &options.user.unwrap());

                let request = ureq::builder().build().post(url.as_str());
                let request =
                    request.set("Authorization", &format!("Token {}", options.password.unwrap()));
                RequestClient::V2(request)
            }
            _ => unreachable!(),
        }
    }
}

/// from https://docs.influxdata.com/influxdb/v1.8/tools/api/#request-body-1,
/// > We recommend writing points in batches of 5,000 to 10,000 points.
/// > Smaller batches, and more HTTP requests, will result in sub-optimal performance.
pub struct Influx {
    request: RequestClient,
    buffer: Vec<Record>,
    measurement: String,
}

impl Influx {
    pub fn new(options: ClientOptions, table: &str, db_type: &Type) -> Influx {
        let request = RequestClient::new(options, db_type);

        Influx {
            request,
            buffer: Vec::with_capacity(BUFFER_SIZE),
            measurement: table.to_string(),
        }
    }

    fn buffer_to_line_protocol(&mut self) -> BytesMut {
        let mut binary_data = BytesMut::new();

        for d in self.buffer.iter() {
            let mut measurement = String::new();

            // push tags
            measurement.push_str(&format!(
                r#"{},id={} sequence={}"#,
                self.measurement,
                d.id.replace(" ", r#"\ "#),
                d.sequence
            ));

            // push fields (optional)
            let o = d.other.as_object().unwrap();
            if !o.is_empty() {
                let mut fields = String::new();
                for (_, map) in o.keys().enumerate() {
                    let prefix = ",";
                    fields.push_str(&format!("{}{}={}", prefix, map, o[map]));
                }
                measurement.push_str(&format!("{}", fields));
            }

            // push timestamp
            measurement.push_str(&format!(" {}\n", d.timestamp.timestamp_millis()));
            binary_data.extend_from_slice(measurement.as_bytes());
        }

        binary_data
    }
}

impl Inserter for Influx {
    fn get_write_buffer(&mut self) -> &mut Vec<Record> {
        &mut self.buffer
    }

    fn end(&mut self) -> Result<(), Error> {
        let request = match self.request.clone() {
            RequestClient::V1(c) => c,
            RequestClient::V2(c) => c,
        };

        request.send_bytes(&self.buffer_to_line_protocol()[..])?;
        self.buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use serde_json::json;

    #[test]
    fn verify_buffer_to_line_protocol_conversion() {
        let json_data = json! {[
            {
                "id": "test val 33 spaces",
                "sequence": 1,
                "timestamp": 1663338562974u64
            },
            {
                "id": "test-val-33",
                "sequence": 2,
                "timestamp": 1663338562974u64,
                "description": "value 2"
            },
            {
                "id": "test-val-33",
                "sequence": 3,
                "timestamp": 1663338562975u64,
                "description": "value3",
                "testing": 789,
                "what":false
            }
        ]};
        let data: Vec<Record> = serde_json::from_value(json_data).unwrap();

        let mut influx = Influx {
            request: RequestClient::V1(ureq::get("test")),
            buffer: data,
            measurement: "measure".to_string(),
        };

        let converted_in_buffer = influx.buffer_to_line_protocol();
        let converted = std::str::from_utf8(&converted_in_buffer).unwrap();

        let expected = indoc! {r#"
            measure,id=test\ val\ 33\ spaces sequence=1 1663338562974
            measure,id=test-val-33 sequence=2,description="value 2" 1663338562974
            measure,id=test-val-33 sequence=3,description="value3",testing=789,what=false 1663338562975
        "#};

        assert_eq!(converted, expected);
    }
}
