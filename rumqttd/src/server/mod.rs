// use tokio::io::{AsyncRead, AsyncWrite};

mod broker;
#[cfg(any(feature = "use-rustls", feature = "use-native-tls"))]
mod tls;

pub use broker::{Broker, LinkType, Server};

// pub trait IO: AsyncRead + AsyncWrite + Send + Sync + Unpin {}
// impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> IO for T {}
