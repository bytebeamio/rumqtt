//! Length-prefixed TCP framing for inter-node communication.
//!
//! Each frame is encoded as a 4-byte big-endian length prefix followed by
//! a UTF-8 payload: `[u32 length][payload bytes]`. This simple protocol
//! allows reliable message boundary detection over a raw TCP stream.

use std::io::{Read, Write};
use std::net::TcpStream;

/// Writes a length-prefixed frame to the stream.
pub(crate) fn write_frame(stream: &mut TcpStream, payload: &str) -> Result<(), std::io::Error> {
    let payload_len = payload.len() as u32;
    stream.write_all(&payload_len.to_be_bytes())?;
    stream.write_all(payload.as_bytes())?;
    stream.flush()?;
    Ok(())
}

/// Reads a length-prefixed frame from the stream (blocking).
///
/// Returns the decoded UTF-8 payload. Fails if the stream closes
/// mid-read or the payload is not valid UTF-8.
pub(crate) fn read_frame(stream: &mut TcpStream) -> Result<String, std::io::Error> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let payload_len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; payload_len];
    stream.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
