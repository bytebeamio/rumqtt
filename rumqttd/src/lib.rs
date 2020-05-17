#[macro_use]
extern crate log;

pub mod tracker;
pub mod link;


use tokio::io::{AsyncRead, AsyncWrite};


pub trait IO: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> IO for T {}
