use std::sync::Arc;

use crate::{config::Record, db::ConnectOptions, db::DatabaseWrite, Error};
use rustls::version::{TLS12, TLS13};
use ureq::Request;
use url::Url;

pub struct Clickhouse {
    request: Request,
}

impl Clickhouse {
    pub fn connect(options: ConnectOptions) -> Clickhouse {
        let agent = match options.secure {
            true => {
                let mut root_store = rustls::RootCertStore::empty();

                let certs = rustls_native_certs::load_native_certs()
                    .expect("Could not load platform certs");
                for cert in certs {
                    // Repackage the certificate DER bytes.
                    let rustls_cert = rustls::Certificate(cert.0);
                    root_store
                        .add(&rustls_cert)
                        .expect("Failed to add native certificate too root store");
                }

                root_store.add_server_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.0.iter().map(
                    |ta| {
                        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                            ta.subject,
                            ta.spki,
                            ta.name_constraints,
                        )
                    },
                ));

                let protocol_versions = &[&TLS12, &TLS13];

                let tls_config = rustls::ClientConfig::builder()
                    .with_safe_default_cipher_suites()
                    .with_safe_default_kx_groups()
                    .with_protocol_versions(protocol_versions)
                    .unwrap()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();

                ureq::builder().tls_config(Arc::new(tls_config)).build()
            }
            false => ureq::builder().build(),
        };

        let mut url = Url::parse(&options.url).expect("TODO");

        url.query_pairs_mut()
            .append_pair("database", &options.database);

        let mut request = agent.post(url.as_str());

        if let Some(user) = &options.user {
            request = request.set("X-ClickHouse-User", user);
        }

        if let Some(password) = &options.password {
            request = request.set("X-ClickHouse-Key", password);
        }

        Clickhouse { request }
    }
}

impl DatabaseWrite for Clickhouse {
    fn write(&mut self, table: &str, buffer: &mut Vec<Record>) -> Result<(), Error> {
        let mut request = self.request.clone();

        let query = format!("INSERT INTO {} FORMAT JSONEachRow", table);
        request = request.query("query", &query);

        let o = serde_json::to_vec::<Vec<Record>>(buffer.as_ref())?;
        request.send_bytes(&o[..])?;

        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
