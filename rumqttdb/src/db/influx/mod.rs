use bytes::BytesMut;
use ureq::Request;
use url::Url;

use crate::{config::Record, db::ConnectOptions, db::DatabaseWrite, Error};

pub struct InfluxV1 {}
impl InfluxV1 {
    pub fn connect(options: ConnectOptions) -> Influx {
        let mut url = Url::parse(&options.url).unwrap().join("write").unwrap();
        url.query_pairs_mut().append_pair("db", &options.database);

        let request = ureq::builder().build().post(url.as_str());
        Influx { request }
    }
}

pub struct InfluxV2 {}
impl InfluxV2 {
    pub fn connect(options: ConnectOptions) -> Influx {
        let mut url = Url::parse(&options.url)
            .unwrap()
            .join("api/v2/write")
            .unwrap();

        url.query_pairs_mut()
            .append_pair("bucket", &options.database);

        url.query_pairs_mut()
            .append_pair("org", &options.user.unwrap());

        let request = ureq::builder().build().post(url.as_str());
        let request = request.set(
            "Authorization",
            &format!("Token {}", options.password.unwrap()),
        );
        Influx { request }
    }
}

pub struct Influx {
    request: Request,
}

impl Influx {
    fn buffer_to_line_protocol(&mut self, table: &str, buffer: &mut Vec<Record>) -> BytesMut {
        let mut binary_data = BytesMut::new();

        for d in buffer.iter() {
            let mut measurement = String::new();

            // push tags
            measurement.push_str(&format!(
                r#"{},id={} sequence={}"#,
                table,
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

impl DatabaseWrite for Influx {
    fn write(&mut self, table: &str, mut payload: &mut Vec<Record>) -> Result<(), Error> {
        let request = self.request.clone();

        request.send_bytes(&self.buffer_to_line_protocol(table, &mut payload)[..])?;

        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
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
        let mut data: Vec<Record> = serde_json::from_value(json_data).unwrap();

        let mut influx = Influx {
            request: ureq::get("test"),
        };

        let converted_in_buffer = influx.buffer_to_line_protocol("measure", &mut data);
        let converted = std::str::from_utf8(&converted_in_buffer).unwrap();

        let expected = indoc! {r#"
            measure,id=test\ val\ 33\ spaces sequence=1 1663338562974
            measure,id=test-val-33 sequence=2,description="value 2" 1663338562974
            measure,id=test-val-33 sequence=3,description="value3",testing=789,what=false 1663338562975
        "#};

        assert_eq!(converted, expected);
    }
}
