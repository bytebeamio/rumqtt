use std::collections::HashMap;

use crate::DatabaseConfig;

#[non_exhaustive]
pub enum Compression {
    None,
    #[cfg(feature = "lz4")]
    Lz4,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "zlib")]
    Zlib,
    #[cfg(feature = "brotli")]
    Brotli,
}

impl Default for Compression {
    #[cfg(feature = "lz4")]
    #[inline]
    fn default() -> Self {
        Compression::Lz4
    }

    #[cfg(not(feature = "lz4"))]
    #[inline]
    fn default() -> Self {
        Compression::None
    }
}

pub struct ConnectOptions {
    pub(crate) url: String,
    pub(crate) database: String,
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) secure: bool,
    compression: Compression,
    options: HashMap<String, String>,
}

impl ConnectOptions {
    pub fn new(url: &str, database: &str) -> ConnectOptions {
        ConnectOptions {
            url: url.to_owned(),
            database: database.to_owned(),
            user: None,
            password: None,
            compression: Compression::default(),
            secure: false,
            options: HashMap::new(),
        }
    }

    pub fn with_config(config: &DatabaseConfig) -> ConnectOptions {
        let mut options = ConnectOptions::new(&config.server, &config.db_name);
        if let Some(username) = &config.user {
            options = options.with_user(username);
        }
        if let Some(password) = &config.password {
            options = options.with_password(password);
        }

        if config.secure {
            options = options.with_security(true);
        };

        options
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
