use native_tls::{Certificate, Identity, Protocol, Result, TlsConnector};

#[derive(Clone)]
pub struct TlsConnectorBuilder {
    identity: Option<Identity>,
    min_protocol: Option<Protocol>,
    max_protocol: Option<Protocol>,
    root_certificates: Vec<Certificate>,
    accept_invalid_certs: bool,
    accept_invalid_hostnames: bool,
    use_sni: bool,
    disable_built_in_roots: bool,
}

impl TlsConnectorBuilder {
    pub fn default() -> TlsConnectorBuilder {
        TlsConnectorBuilder {
            identity: None,
            min_protocol: Some(Protocol::Tlsv10),
            max_protocol: None,
            root_certificates: vec![],
            use_sni: true,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
            disable_built_in_roots: false,
        }
    }

    /// Sets the identity to be used for client certificate authentication.
    pub fn identity(&mut self, identity: Identity) -> &mut TlsConnectorBuilder {
        self.identity = Some(identity);
        self
    }

    /// Sets the minimum supported protocol version.
    ///
    /// A value of `None` enables support for the oldest protocols supported by the implementation.
    ///
    /// Defaults to `Some(Protocol::Tlsv10)`.
    pub fn min_protocol_version(&mut self, protocol: Option<Protocol>) -> &mut TlsConnectorBuilder {
        self.min_protocol = protocol;
        self
    }

    /// Sets the maximum supported protocol version.
    ///
    /// A value of `None` enables support for the newest protocols supported by the implementation.
    ///
    /// Defaults to `None`.
    pub fn max_protocol_version(&mut self, protocol: Option<Protocol>) -> &mut TlsConnectorBuilder {
        self.max_protocol = protocol;
        self
    }

    /// Adds a certificate to the set of roots that the connector will trust.
    ///
    /// The connector will use the system's trust root by default. This method can be used to add
    /// to that set when communicating with servers not trusted by the system.
    ///
    /// Defaults to an empty set.
    pub fn add_root_certificate(&mut self, cert: Certificate) -> &mut TlsConnectorBuilder {
        self.root_certificates.push(cert);
        self
    }

    /// Controls the use of built-in system certificates during certificate validation.
    ///
    /// Defaults to `false` -- built-in system certs will be used.
    pub fn disable_built_in_roots(&mut self, disable: bool) -> &mut TlsConnectorBuilder {
        self.disable_built_in_roots = disable;
        self
    }

    /// Controls the use of certificate validation.
    ///
    /// Defaults to `false`.
    ///
    /// # Warning
    ///
    /// You should think very carefully before using this method. If invalid certificates are trusted, *any*
    /// certificate for *any* site will be trusted for use. This includes expired certificates. This introduces
    /// significant vulnerabilities, and should only be used as a last resort.
    pub fn danger_accept_invalid_certs(
        &mut self,
        accept_invalid_certs: bool,
    ) -> &mut TlsConnectorBuilder {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    /// Controls the use of Server Name Indication (SNI).
    ///
    /// Defaults to `true`.
    pub fn use_sni(&mut self, use_sni: bool) -> &mut TlsConnectorBuilder {
        self.use_sni = use_sni;
        self
    }

    /// Controls the use of hostname verification.
    ///
    /// Defaults to `false`.
    ///
    /// # Warning
    ///
    /// You should think very carefully before using this method. If invalid hostnames are trusted, *any* valid
    /// certificate for *any* site will be trusted for use. This introduces significant vulnerabilities, and should
    /// only be used as a last resort.
    pub fn danger_accept_invalid_hostnames(
        &mut self,
        accept_invalid_hostnames: bool,
    ) -> &mut TlsConnectorBuilder {
        self.accept_invalid_hostnames = accept_invalid_hostnames;
        self
    }

    pub fn build(&self) -> Result<TlsConnector> {
        let clone = self.clone();
        let TlsConnectorBuilder {
            identity,
            min_protocol,
            max_protocol,
            root_certificates,
            accept_invalid_certs,
            accept_invalid_hostnames,
            use_sni,
            disable_built_in_roots,
        } = clone;
        let mut builder = TlsConnector::builder();
        if let Some(identity) = identity {
            builder.identity(identity);
        }
        builder
            .min_protocol_version(min_protocol)
            .max_protocol_version(max_protocol)
            .danger_accept_invalid_certs(accept_invalid_certs)
            .danger_accept_invalid_hostnames(accept_invalid_hostnames)
            .use_sni(use_sni)
            .disable_built_in_roots(disable_built_in_roots);

        for cert in root_certificates {
            builder.add_root_certificate(cert);
        }
        builder.build()
    }
}
