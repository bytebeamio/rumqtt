use crate::Compression;
use std::collections::HashMap;

pub struct ClientOptions {
    pub(crate) url: String,
    pub(crate) database: String,
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) secure: bool,
    compression: Compression,
    options: HashMap<String, String>,
}

impl ClientOptions {
    pub fn new(url: &str, database: &str) -> ClientOptions {
        ClientOptions {
            url: url.to_owned(),
            database: database.to_owned(),
            user: None,
            password: None,
            compression: Compression::default(),
            secure: false,
            options: HashMap::new(),
        }
    }

    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_option(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }

    pub fn with_security(mut self, security: bool) -> Self {
        self.secure = security;
        self
    }
}
